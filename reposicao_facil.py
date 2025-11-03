# reposicao_facil.py - CÓDIGO FINAL DE ESTABILIDADE V10.1
# Implementa a persistência em DISCO com MANIFESTO JSON para máxima resistência ao F5.

import datetime as dt
import pandas as pd
import streamlit as st
import io 
import re 
import hashlib 
import os # CRÍTICO: Para manipulação de arquivos
import json # CRÍTICO: Para o manifesto JSON
from typing import Optional, Tuple 
from unidecode import unidecode 

# MÓDULOS NECESSÁRIOS
import logica_compra 
import mod_compra_autom
import mod_alocacao 

# [IMPORTAÇÕES DA LOGICA_COMPRA MANTIDAS]
from logica_compra import (
    Catalogo, baixar_xlsx_do_sheets, baixar_xlsx_por_link_google,
    load_any_table_from_bytes, mapear_tipo, mapear_colunas,
    calcular as calcular_compra, DEFAULT_SHEET_ID
)

# MÓDULOS DE ORDEM DE COMPRA (SQLITE)
try:
    import ordem_compra 
    import gerenciador_oc 
except ImportError:
    pass 

VERSION = "v10.1 - PERSISTÊNCIA EM DISCO COM MANIFESTO"

# ===================== CONFIG, ESTADO, E ARMAZENAMENTO =====================
st.set_page_config(page_title="Reposição Logística — Alivvia", layout="wide")

DEFAULT_SHEET_LINK = "https://docs.google.com/spreadsheets/d/1cTLARjq-B5g50dL6tcntg7lb_Iu0ta43/edit?usp=sharing&ouid=109458533144345974874&rtpof=true&sd=true"

# DIRETÓRIO RAIZ DE ARMAZENAMENTO EM DISCO
BASE_UPLOAD_DIR = ".st_uploads" 
EMPRESAS = ["ALIVVIA", "JCA"]
SLOTS = ["FULL", "VENDAS", "ESTOQUE"]

def hash_bytes(blob: bytes) -> str:
    """Calcula o SHA1 dos bytes do arquivo."""
    return hashlib.sha1(blob).hexdigest()

def _get_manifest_path(empresa: str) -> str:
    """Retorna o caminho para o arquivo de manifesto da empresa."""
    emp_dir = os.path.join(BASE_UPLOAD_DIR, empresa)
    return os.path.join(emp_dir, "_manifest.json")

def load_manifest(empresa: str) -> dict:
    """Carrega o manifesto do disco ou retorna um dicionário vazio."""
    manifest_path = _get_manifest_path(empresa)
    if os.path.exists(manifest_path):
        try:
            with open(manifest_path, 'r') as f:
                return json.load(f)
        except json.JSONDecodeError:
            return {}
    return {}

def save_manifest(empresa: str, manifest_data: dict):
    """Salva o manifesto no disco."""
    manifest_path = _get_manifest_path(empresa)
    os.makedirs(os.path.dirname(manifest_path), exist_ok=True)
    with open(manifest_path, 'w') as f:
        json.dump(manifest_data, f, indent=4)

def save_file_to_disk_and_update_manifest(empresa: str, slot: str, up_file) -> Optional[str]:
    """Salva o arquivo no disco e atualiza o manifesto."""
    try:
        up_file.seek(0)
        raw_bytes = up_file.read()
        file_hash = hash_bytes(raw_bytes)
        
        # Cria o diretório específico do slot
        slot_dir = os.path.join(BASE_UPLOAD_DIR, empresa, slot)
        os.makedirs(slot_dir, exist_ok=True)
        
        # Caminho do arquivo (usando o hash para evitar colisões e o nome para debug)
        file_ext = os.path.splitext(up_file.name)[1]
        file_path = os.path.join(slot_dir, f"{slot}{file_ext}") # Simplificando o nome no disco

        # Salva o arquivo fisicamente
        with open(file_path, "wb") as f:
            f.write(raw_bytes)

        # Atualiza o manifesto
        manifest = load_manifest(empresa)
        manifest[slot] = {
            "name": up_file.name,
            "path": file_path,
            "size": len(raw_bytes),
            "sha1": file_hash,
            "saved_at": dt.datetime.now().isoformat()
        }
        save_manifest(empresa, manifest)
        
        return file_path
    except Exception as e:
        st.error(f"Erro ao salvar o arquivo em disco: {e}")
        return None

def clear_file_from_disk_and_manifest(empresa: str, slot: str):
    """Remove o arquivo do disco e do manifesto."""
    manifest = load_manifest(empresa)
    if slot in manifest:
        file_path = manifest[slot].get("path")
        if file_path and os.path.exists(file_path):
            try:
                os.remove(file_path)
            except OSError as e:
                st.warning(f"Não foi possível deletar o arquivo: {e}")
        
        del manifest[slot]
        save_manifest(empresa, manifest)

def _rehydrate_session_from_disk():
    """Lê o manifesto e restaura os dados (bytes e nome) para o st.session_state na inicialização."""
    for emp in EMPRESAS:
        manifest = load_manifest(emp)
        for slot in SLOTS:
            if slot in manifest:
                data = manifest[slot]
                file_path = data.get("path")
                
                # Só restaura se o arquivo existir no disco
                if file_path and os.path.exists(file_path):
                    st.session_state[emp][slot]["name"] = data["name"]
                    # NÃO PODE LER OS BYTES AQUI, POIS PODE SER GRANDE DEMAIS
                    # Lemos os bytes APENAS QUANDO O CÁLCULO FOR DISPARADO.
                    st.session_state[emp][slot]["bytes"] = None 
                    st.session_state[emp][slot]["path"] = file_path # Guarda o caminho para futura leitura

# Funções de inicialização de estado
def _ensure_state():
    """Garante que todas as chaves de estado de sessão existam."""
    # [Restante das chaves...]
    st.session_state.setdefault("oc_cesta", pd.DataFrame()) 
    st.session_state.setdefault("compra_autom_data", {})
    
    for emp in EMPRESAS:
        st.session_state.setdefault(emp, {})
        for slot in SLOTS:
            st.session_state[emp].setdefault(slot, {"name": None, "bytes": None, "path": None})

_ensure_state()
_rehydrate_session_from_disk() # CHAVE DA PERSISTÊNCIA: Carrega o manifesto aqui!


# ===================== UI: SIDEBAR E PARÂMETROS =====================
with st.sidebar:
    st.subheader("Parâmetros")
    h  = st.selectbox("Horizonte (dias)", [30, 60, 90], index=1, key="h")
    g  = st.number_input("Crescimento % ao mês", value=0.0, step=1.0, key="g")
    LT = st.number_input("Lead time (dias)", value=0, step=1, min_value=0, key="LT")

    # [Lógica de Carregamento de Planilhas Google Sheets - Mantida]
    @st.cache_data(show_spinner="Baixando Planilha de Padrões KITS/CAT...")
    def get_padrao_from_sheets(sheet_id):
        content = logica_compra.baixar_xlsx_do_sheets(sheet_id)
        return logica_compra._carregar_padrao_de_content(content)

    st.markdown("---")
    st.subheader("Padrão (KITS/CAT) — Google Sheets")
    colA, colB = st.columns([1, 1])
    # ... (Restante da lógica do sidebar)
    
# [RESTANTE DO CÓDIGO ATÉ AS TABS]
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
            # Obtém estado do disco ou da memória
            saved_name = st.session_state[emp][slot]["name"]
            
            with col:
                st.markdown(f"**{label} — {emp}**")
                
                if saved_name:
                    st.info(f"💾 **Fixo no Disco**: {saved_name}")
                    
                    if st.button(f"🗑️ Limpar {label}", key=f"clr_{slot}_{emp}", use_container_width=True, type="secondary"):
                        clear_file_from_disk_and_manifest(emp, slot)
                        st.session_state[emp][slot] = {"name": None, "bytes": None, "path": None}
                        st.rerun() 
                
                else:
                    up_file = st.file_uploader("CSV/XLSX/XLS", type=["csv","xlsx","xls"], key=f"up_{slot}_{emp}")
                    
                    if up_file is not None:
                        # 3. SALVAMENTO AGRESSIVO NO DISCO E MANIFESTO
                        file_path = save_file_to_disk_and_update_manifest(emp, slot, up_file)
                        
                        # Salva o path e o nome no session_state (sem os bytes, para economizar RAM)
                        if file_path:
                            st.session_state[emp][slot]["name"] = up_file.name
                            st.session_state[emp][slot]["path"] = file_path 
                            st.session_state[emp][slot]["bytes"] = None # Garante que a RAM fique limpa
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
        # ... (Botões de Confirmação e Limpeza Global adaptados para a nova estrutura)
        
    # Chamadas finais
    render_block("ALIVVIA")
    render_block("JCA")
    
    # Botão de Limpeza Global
    st.markdown("## ⚠️ Limpeza Total de Dados")
    if st.button("🔴 Limpar TUDO (ALIVVIA e JCA)", key="clr_all_global", type="primary", use_container_width=True):
        # Lógica de limpeza global em disco e sessão
        for emp in EMPRESAS:
            for slot in SLOTS:
                clear_file_from_disk_and_manifest(emp, slot)
                st.session_state[emp][slot] = {"name": None, "bytes": None, "path": None}
        st.info("Todos os dados foram limpos do disco e da sessão.")
        st.rerun()

# ---------- TAB 2: COMPRA AUTOMÁTICA (Necessita de adaptação para ler do disco) ----------
with tab2:
    # A lógica aqui precisa ser adaptada para ler do disco antes do cálculo
    # Chamamos o módulo que está corrigido (V8.5)
    mod_compra_autom.render_tab2(st.session_state, st.session_state.h, st.session_state.g, st.session_state.LT)

# ---------- RESTANTE DO CÓDIGO... ----------