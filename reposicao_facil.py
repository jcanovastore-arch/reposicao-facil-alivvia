# reposicao_facil.py - CÓDIGO FINAL DE ESTABILIDADE V10.0
# Implementa a persistência em DISCO para resolver o problema do sumiço no F5.

import datetime as dt
import pandas as pd
import streamlit as st
import io 
import re 
import hashlib 
import os # NOVO: Para manipulação de arquivos
from dataclasses import dataclass 
from typing import Optional, Tuple 
import numpy as np 
from unidecode import unidecode 
import requests 
from requests.adapters import HTTPAdapter, Retry 

# MÓDULOS MODULARIZADOS
import logica_compra 
import mod_compra_autom
import mod_alocacao 

# Importando funções e constantes do módulo de lógica
from logica_compra import (
    Catalogo,
    baixar_xlsx_do_sheets,
    baixar_xlsx_por_link_google,
    load_any_table_from_bytes,
    mapear_tipo,
    mapear_colunas,
    calcular as calcular_compra,
    DEFAULT_SHEET_ID
)

# MÓDULOS DE ORDEM DE COMPRA (SQLITE)
try:
    import ordem_compra 
    import gerenciador_oc 
except ImportError:
    pass 

VERSION = "v10.0 - PERSISTÊNCIA EM DISCO FINAL"

# ===================== CONFIG E ESTADO =====================
st.set_page_config(page_title="Reposição Logística — Alivvia", layout="wide")

DEFAULT_SHEET_LINK = "https://docs.google.com/spreadsheets/d/1cTLARjq-B5g50dL6tcntg7lb_Iu0ta43/edit?usp=sharing&ouid=109458533144345974874&rtpof=true&sd=true"

# DIRETÓRIO DE ARMAZENAMENTO EM DISCO (Streamlit Cloud permite escrita)
UPLOAD_DIR = ".st_uploads" 

# Hashing function
def hash_bytes(blob: bytes) -> str:
    return hashlib.sha256(blob).hexdigest()

# Função para salvar o arquivo em disco
def save_file_to_disk(blob: bytes, file_name: str, file_hash: str) -> str:
    os.makedirs(UPLOAD_DIR, exist_ok=True)
    file_path = os.path.join(UPLOAD_DIR, f"{file_hash}_{file_name}")
    with open(file_path, "wb") as f:
        f.write(blob)
    return file_path

# Função para carregar o arquivo do disco
def load_file_from_disk(file_path: str) -> Optional[bytes]:
    if file_path and os.path.exists(file_path):
        with open(file_path, "rb") as f:
            return f.read()
    return None


def _ensure_state():
    """Garante que todas as chaves de estado de sessão existam."""
    st.session_state.setdefault("catalogo_df", None)
    st.session_state.setdefault("kits_df", None)
    st.session_state.setdefault("loaded_at", None)
    st.session_state.setdefault("alt_sheet_link", DEFAULT_SHEET_LINK)
    st.session_state.setdefault("oc_cesta", pd.DataFrame()) 
    st.session_state.setdefault("compra_autom_data", {})
    
    for emp in ["ALIVVIA", "JCA"]:
        st.session_state.setdefault(emp, {})
        # Adicionado 'path' para salvar o caminho no disco
        st.session_state[emp].setdefault("FULL",   {"name": None, "bytes": None, "path": None})
        st.session_state[emp].setdefault("VENDAS", {"name": None, "bytes": None, "path": None})
        st.session_state[emp].setdefault("ESTOQUE",{"name": None, "bytes": None, "path": None})

_ensure_state()

# ===================== UI: SIDEBAR E PARÂMETROS =====================
with st.sidebar:
    st.subheader("Parâmetros")
    h  = st.selectbox("Horizonte (dias)", [30, 60, 90], index=1, key="h")
    g  = st.number_input("Crescimento % ao mês", value=0.0, step=1.0, key="g")
    LT = st.number_input("Lead time (dias)", value=0, step=1, min_value=0, key="LT")

    st.markdown("---")
    st.subheader("Padrão (KITS/CAT) — Google Sheets")
    st.caption("Carrega **somente** quando você clicar.")
    
    @st.cache_data(show_spinner="Baixando Planilha de Padrões KITS/CAT...")
    def get_padrao_from_sheets(sheet_id):
        content = logica_compra.baixar_xlsx_do_sheets(sheet_id)
        return logica_compra._carregar_padrao_de_content(content)

    colA, colB = st.columns([1, 1])
    with colA:
        if st.button("Carregar padrão agora", use_container_width=True):
            try:
                cat = get_padrao_from_sheets(DEFAULT_SHEET_ID)
                st.session_state.catalogo_df = cat.catalogo_simples.rename(columns={"component_sku":"sku"})
                st.session_state.kits_df = cat.kits_reais
                st.session_state.loaded_at = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                st.success("Padrão carregado com sucesso.")
            except Exception as e:
                st.session_state.catalogo_df = None; st.session_state.kits_df = None; st.session_state.loaded_at = None
                st.error(str(e))
    with colB:
        st.link_button("🔗 Abrir no Drive (editar)", DEFAULT_SHEET_LINK, use_container_width=True)

    st.text_input("Link alternativo do Google Sheets (opcional)", key="alt_sheet_link",
                  help="Se necessário, cole o link e use o botão abaixo.")
    if st.button("Carregar deste link", use_container_width=True):
        try:
            content = logica_compra.baixar_xlsx_por_link_google(st.session_state.alt_sheet_link.strip())
            cat = logica_compra._carregar_padrao_de_content(content)
            st.session_state.catalogo_df = cat.catalogo_simples.rename(columns={"component_sku":"sku"})
            st.session_state.kits_df = cat.kits_reais
            st.session_state.loaded_at = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            st.success("Padrão carregado (link alternativo).")
        except Exception as e:
            st.session_state.catalogo_df = None; st.session_state.kits_df = None; st.session_state.loaded_at = None
            st.error(str(e))
            
# ===================== TÍTULO E ABAS =====================
st.title("Reposição Logística — Alivvia")
if st.session_state.catalogo_df is None or st.session_state.kits_df is None:
    st.warning("► Carregue o **Padrão (KITS/CAT)** no sidebar antes de usar as abas.")

tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📂 Dados das Empresas", 
    "🧮 Compra Automática", 
    "📦 Alocação de Compra", 
    "🛒 Ordem de Compra (OC)", 
    "✨ Gerenciador de OCs"
])

# ---------- TAB 1: UPLOADS (LÓGICA DE PERSISTÊNCIA EM DISCO) ----------
with tab1:
    st.subheader("Uploads fixos por empresa (os arquivos permanecem salvos após F5)")
    st.caption("O arquivo é salvo **em disco** no servidor para garantir a persistência (o box azul confirma).")

    def render_block(emp: str):
        st.markdown(f"### {emp}")
        
        def render_upload_slot(slot: str, label: str, col):
            saved_name = st.session_state[emp][slot]["name"]
            saved_path = st.session_state[emp][slot].get("path")
            
            with col:
                st.markdown(f"**{label} — {emp}**")
                
                # 1. VERIFICA E CARREGA DO DISCO SE EXISTIR
                if saved_path and os.path.exists(saved_path):
                    # Garante que os bytes estejam no session_state (lê do disco)
                    if st.session_state[emp][slot]["bytes"] is None:
                         st.session_state[emp][slot]["bytes"] = load_file_from_disk(saved_path)

                    st.info(f"💾 **Fixo no Disco**: {saved_name}")
                    
                    # --- BOTÃO DE LIMPEZA INDIVIDUAL ---
                    if st.button(f"🗑️ Limpar {label}", key=f"clr_{slot}_{emp}", use_container_width=True, type="secondary"):
                        try: os.remove(saved_path)
                        except OSError: pass # Ignora se falhar
                        st.session_state[emp][slot] = {"name": None, "bytes": None, "path": None}
                        st.rerun() 
                
                else:
                    # 2. FILE UPLOADER
                    up_file = st.file_uploader("CSV/XLSX/XLS", type=["csv","xlsx","xls"], key=f"up_{slot}_{emp}")
                    
                    if up_file is not None:
                        # 3. SALVAMENTO AGRESSIVO (DISK)
                        raw_bytes = up_file.read()
                        file_hash = hash_bytes(raw_bytes)

                        file_path = save_file_to_disk(raw_bytes, up_file.name, file_hash)
                        
                        # Salva o path e o nome no session_state (o path é a chave da persistência)
                        st.session_state[emp][slot]["bytes"] = raw_bytes 
                        st.session_state[emp][slot]["name"] = up_file.name
                        st.session_state[emp][slot]["path"] = file_path 
                        st.rerun() 

        # Renderizar slots
        col_full, col_vendas = st.columns(2)
        render_upload_slot("FULL", "FULL", col_full)
        render_upload_slot("VENDAS", "Shopee/MT (Vendas)", col_vendas)

        st.markdown("---")
        col_estoque, _ = st.columns([1,1])
        render_upload_slot("ESTOQUE", "Estoque Físico", col_estoque)
        st.markdown("___") 
        
        # --- Botões de Ação ---
        c3, c4 = st.columns([1, 1])

        with c3:
            if st.button(f"Salvar {emp} (Confirmar)", use_container_width=True, key=f"save_{emp}", type="primary"):
                st.success(f"Status {emp} confirmado: Arquivos estão na sessão/disco.")
        
        with c4:
            if st.button(f"Limpar {emp}", use_container_width=True, key=f"clr_{emp}", type="secondary"):
                for s in ["FULL", "VENDAS", "ESTOQUE"]:
                    if st.session_state[emp][s].get("path"):
                        try: os.remove(st.session_state[emp][s]["path"])
                        except OSError: pass
                st.session_state[emp] = {"FULL":{"name":None,"bytes":None,"path":None},
                                         "VENDAS":{"name":None,"bytes":None,"path":None},
                                         "ESTOQUE":{"name":None,"bytes":None,"path":None}}
                st.info(f"{emp} limpo.")
                st.rerun() 

        st.markdown("___") 

    # Chamadas finais
    render_block("ALIVVIA")
    render_block("JCA")
    
    # Botão de Limpeza Global
    st.markdown("## ⚠️ Limpeza Total de Dados")
    if st.button("🔴 Limpar TUDO (ALIVVIA e JCA)", key="clr_all_global", type="primary", use_container_width=True):
        for emp in ["ALIVVIA", "JCA"]:
            for s in ["FULL", "VENDAS", "ESTOQUE"]:
                if st.session_state[emp][s].get("path"):
                    try: os.remove(st.session_state[emp][s]["path"])
                    except OSError: pass
            st.session_state[emp] = {"FULL":{"name":None,"bytes":None,"path":None},
                                     "VENDAS":{"name":None,"bytes":None,"path":None},
                                     "ESTOQUE":{"name":None,"bytes":None,"path":None}}
        st.info("Todos os dados foram limpos.")
        st.rerun()

# ---------- TAB 2: COMPRA AUTOMÁTICA ----------
with tab2:
    mod_compra_autom.render_tab2(st.session_state, st.session_state.h, st.session_state.g, st.session_state.LT)

# ---------- TAB 3: ALOCAÇÃO DE COMPRA ----------
with tab3:
    mod_alocacao.render_tab3(st.session_state)
    
# ... (Restante das Tabs 4 e 5)

st.caption("© Alivvia — simples, robusto e auditável. (V10.0)")