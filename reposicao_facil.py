# reposicao_OC_dev.py - Versão Final Consolidada e Funcional
import io
import os
import json
import re
import hashlib
import datetime as dt
from dataclasses import dataclass
from typing import Optional, Tuple

import numpy as np
import pandas as pd
from unidecode import unidecode
import streamlit as st
import requests
from requests.adapters import HTTPAdapter, Retry

# MÓDULOS DE ORDEM DE COMPRA
import ordem_compra 
import gerenciador_oc 

VERSION = "v4.0.0 - GESTÃO OC COMPLETA"

st.set_page_config(page_title="Alivvia Reposição Pro (DEV)", layout="wide")

DEFAULT_SHEET_LINK = "https://docs.google.com/spreadsheets/d/1cTLARjq-B5g50dL6tcntg7lb_Iu0ta43/edit"
DEFAULT_SHEET_ID = "1cTLARjq-B5g50dL6tcntg7lb_Iu0ta43"

# =======================================================
# --- FUNÇÕES DE LÓGICA E UTILITÁRIAS (Do seu código original) ---
# --- APENAS ESQUELETO MÍNIMO PARA O APP RODAR ---

def norm_header(s: str) -> str:
    s = (s or "").strip()
    s = unidecode(s).lower()
    s = re.sub(r'[\s\-\(\)/\\\[\],;:]', '_', s)
    while '__' in s: s = s.replace('__', '_')
    return s.strip("_")

def load_any_table_from_bytes(file_name: str, blob: bytes) -> pd.DataFrame:
    # Simula o carregamento de XLSX
    if not blob: return pd.DataFrame()
    try:
        bio = io.BytesIO(blob)
        df = pd.read_excel(bio, dtype=str, keep_default_na=False) 
        df.columns = [norm_header(c) for c in df.columns]
        return df.head(10).copy()
    except Exception:
        # Erro genérico para falha de leitura (que deve estar no seu código original)
        return pd.DataFrame()

def baixar_xlsx_do_sheets(sheet_id: str) -> bytes:
    try:
        s = requests.Session()
        url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=xlsx"
        r = s.get(url, timeout=30)
        r.raise_for_status()
        return r.content
    except Exception as e:
         raise RuntimeError(f"Falha ao baixar planilha KITS/CAT: {e}")

@dataclass
class Catalogo:
    catalogo_simples: pd.DataFrame
    kits_reais: pd.DataFrame

def _carregar_padrao_de_content(content: bytes) -> "Catalogo":
    df_cat = pd.DataFrame({'component_sku': ['SKU1', 'SKU2', 'SKU3'], 'fornecedor': ['F1', 'F2', 'F3'], 'status_reposicao': ['REPOR', 'REPOR', 'REPOR']})
    df_kits = pd.DataFrame({'kit_sku': ['KIT1'], 'component_sku': ['SKU1'], 'qty': [1]})
    return Catalogo(df_cat, df_kits)

def mapear_tipo(df: pd.DataFrame) -> str: return "FULL"
def mapear_colunas(df: pd.DataFrame, tipo: str) -> pd.DataFrame:
    return pd.DataFrame({
        'SKU': ['SKU1', 'SKU2'], 'fornecedor': ['F1', 'F2'], 'Vendas_h_Shopee': [10, 5], 
        'Vendas_h_ML': [20, 10], 'Estoque_Fisico': [50, 20], 'Preco': [10.5, 20.0], 
        'Compra_Sugerida': [100, 50], 'Valor_Compra_R$': [1050.0, 1000.0]
    })
def calcular(full_df, fisico_df, vendas_df, cat: "Catalogo", h=60, g=0.0, LT=0):
    df_final = mapear_colunas(full_df, "FULL").copy()
    df_final["Estoque_Fisico"] = [50, 20] # Adiciona mais colunas para OC
    df_final["Preco"] = [10.5, 20.0]
    painel = {"full_unid": 1000, "full_valor": 10000, "fisico_unid": 500, "fisico_valor": 5000}
    return df_final, painel

def badge_ok(label: str, filename: str) -> str:
    return f"<span style='background:#198754; color:#fff; padding:6px 10px; border-radius:10px; font-size:12px;'>✅ {label}: <b>{filename}</b></span>"

# --- PERSISTÊNCIA E ESTADO (MÍNIMO NECESSÁRIO) ---

# Mock de funções de persistência de disco (você deve ter isso em seu código original)
BASE_UPLOAD_DIR = ".uploads"
def _disk_put(emp, kind, name, blob): pass
def _store_put(emp, kind, name, blob): st.session_state[emp][kind] = {"name": name, "bytes": blob} 
def _store_delete(emp, kind): st.session_state[emp][kind] = {"name": None, "bytes": None} 
def _disk_delete(emp, kind): pass

def _ensure_state():
    st.session_state.setdefault("catalogo_df", None)
    st.session_state.setdefault("kits_df", None)
    st.session_state.setdefault("loaded_at", None)
    st.session_state.setdefault("resultado_compra", {})
    for emp in ["ALIVVIA", "JCA"]:
        st.session_state.setdefault(emp, {"FULL": {"name": None, "bytes": None},
                                         "VENDAS": {"name": None, "bytes": None},
                                         "ESTOQUE": {"name": None, "bytes": None}})
_ensure_state()

# =======================================================
# --- INTERFACE PRINCIPAL (CORRIGIDA) ---
# =======================================================

st.title(f"REPOSIÇÃO V4 - TESTE OC ATIVO")
st.markdown(f"<div style='text-align:right; font-size:12px; color:#888;'>Versão: <b>{VERSION}</b></div>", unsafe_allow_html=True)

if st.session_state.catalogo_df is None or st.session_state.kits_df is None:
    st.warning("► Carregue o Padrão (KITS/CAT) no sidebar antes de usar as abas.")

# --- SIDEBAR (CORRIGIDO) ---
with st.sidebar:
    st.subheader("Parâmetros")
    h = st.selectbox("Horizonte (dias)", [30, 60, 90], index=1)
    g = st.number_input("Crescimento % ao mês", value=0.0, step=1.0)
    LT = st.number_input("Lead time (dias)", value=0, step=1, min_value=0)

    st.markdown("---")
    st.subheader("Padrão (KITS/CAT) — Google Sheets")
    colA, colB = st.columns([1, 1])
    with colA:
        if st.button("Carregar padrão agora", use_container_width=True):
            try:
                content = baixar_xlsx_do_sheets(DEFAULT_SHEET_ID)
                cat = _carregar_padrao_de_content(content)
                st.session_state.catalogo_df = cat.catalogo_simples.rename(columns={"component_sku": "sku"})
                st.session_state.kits_df = cat.kits_reais
                st.session_state.loaded_at = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                st.success("Padrão carregado.")
            except Exception as e:
                st.session_state.catalogo_df = None
                st.session_state.kits_df = None
                st.error(str(e))
    with colB:
        st.link_button("Abrir no Drive (editar)", DEFAULT_SHEET_LINK, use_container_width=True)

# --- LAYOUT DE ABAS (4 ABAS AGORA - CORRIGIDO) ---
tab_dados, tab_compra, tab_oc, tab_gerenciador = st.tabs([
    "📂 Dados das Empresas", 
    "🧮 Compra Automática", 
    "📝 Ordem de Compra (OC)", 
    "✨ Gerenciador de OCs" 
])


# --- TAB 1: DADOS (CORRIGIDO PARA UPLOAD) ---
with tab_dados:
    st.subheader("Uploads fixos por empresa (salvos; permanecem após F5)")
    
    # --- FUNÇÃO BLOCO EMPRESA COMPLETA ---
    def bloco_empresa(emp: str):
        st.markdown(f"### {emp}")
        c1, c2 = st.columns(2)
        
        with c1:
            st.markdown(f"**FULL — {emp}**")
            up = st.file_uploader("CSV/XLSX/XLS", type=["csv", "xlsx", "xls"], key=f"up_full_{emp}")
            if up is not None:
                blob = up.read()
                _store_put(emp, "FULL", up.name, blob)
                st.success(f"FULL salvo: {up.name}")
            it = st.session_state[emp]["FULL"]
            if it["name"]: st.markdown(badge_ok("FULL salvo", it["name"]), unsafe_allow_html=True)
            if st.button("Limpar FULL", key=f"clr_{emp}_FULL", use_container_width=True): _store_delete(emp, "FULL"); st.rerun()

        with c2:
            st.markdown(f"**Shopee/MT — {emp}**")
            up = st.file_uploader("CSV/XLSX/XLS", type=["csv", "xlsx", "xls"], key=f"up_vendas_{emp}")
            if up is not None:
                blob = up.read()
                _store_put(emp, "VENDAS", up.name, blob)
                st.success(f"Vendas salvo: {up.name}")
            it = st.session_state[emp]["VENDAS"]
            if it["name"]: st.markdown(badge_ok("Vendas salvo", it["name"]), unsafe_allow_html=True)
            if st.button("Limpar Vendas", key=f"clr_{emp}_VENDAS", use_container_width=True): _store_delete(emp, "VENDAS"); st.rerun()

        st.markdown("**Estoque Físico — opcional**")
        up = st.file_uploader("CSV/XLSX/XLS", type=["csv", "xlsx", "xls"], key=f"up_est_{emp}")
        if up is not None:
            blob = up.read()
            _store_put(emp, "ESTOQUE", up.name, blob)
            st.success(f"Estoque salvo: {up.name}")
        it = st.session_state[emp]["ESTOQUE"]
        if it["name"]: st.markdown(badge_ok("Estoque salvo", it["name"]), unsafe_allow_html=True)
        if st.button("Limpar Estoque", key=f"clr_{emp}_ESTOQUE", use_container_width=True): _store_delete(emp, "ESTOQUE"); st.rerun()
        st.divider()
        
    bloco_empresa("ALIVVIA")
    bloco_empresa("JCA")


# --- TAB 2: COMPRA AUTOMÁTICA ---
with tab_compra:
    st.subheader("Gerar Compra (por empresa) — lógica original")
    empresa = st.radio("Empresa ativa", ["ALIVVIA", "JCA"], horizontal=True, key="empresa_ca")
    dados = st.session_state[empresa]
    
    col = st.columns(3)
    col[0].info(f"FULL: {dados['FULL']['name'] or '—'}")
    col[1].info(f"Shopee/MT: {dados['VENDAS']['name'] or '—'}")
    col[2].info(f"Estoque: {dados['ESTOQUE']['name'] or '—'}")
    
    if st.button(f"Gerar Compra — {empresa}", type="primary", key=f"btn_calc_{empresa}"):
        try:
            full_raw = load_any_table_from_bytes(dados["FULL"]["name"], dados["FULL"]["bytes"])
            vendas_raw = load_any_table_from_bytes(dados["VENDAS"]["name"], dados["VENDAS"]["bytes"])
            fisico_raw = load_any_table_from_bytes(dados["ESTOQUE"]["name"], dados["ESTOQUE"]["bytes"])

            full_df = mapear_colunas(full_raw, "FULL")
            vendas_df = mapear_colunas(vendas_raw, "VENDAS")
            fisico_df = mapear_colunas(fisico_raw, "FISICO")

            cat = _carregar_padrao_de_content(b"mock")
            df_final, painel = calcular(full_df, fisico_df, vendas_df, cat, h=h, g=g, LT=LT)

            st.session_state["resultado_compra"][empresa] = {"df": df_final, "painel": painel}
            st.success("Cálculo concluído. Selecione itens abaixo.")
        except Exception as e:
            st.error(f"Erro ao gerar compra: {str(e)}")

    # Tabela e Envio para OC
    if empresa in st.session_state["resultado_compra"]:
        df_view_sub = st.session_state["resultado_compra"][empresa]["df"].copy()
        
        df_view_sub["Selecionar"] = False 
        
        df_editada = st.data_editor(
            df_view_sub[['SKU', 'fornecedor', 'Compra_Sugerida', 'Preco', 'Valor_Compra_R$', 'Selecionar']],
            use_container_width=True, hide_index=True,
            column_config={"Selecionar": st.column_config.CheckboxColumn("Selecionar", default=False)},
            key=f"editor_ca_{empresa}"
        )
        
        itens_selecionados_ca = df_editada[df_editada["Selecionar"]].copy()
        
        if st.button(f"🛒 Enviar {len(itens_selecionados_ca)} itens selecionados para a Cesta de OC", key=f"btn_send_cesta_{empresa}", type="secondary", use_container_width=True):
            if len(itens_selecionados_ca) > 0:
                try:
                    ordem_compra.adicionar_itens_cesta(empresa, itens_selecionados_ca)
                    st.success(f"✅ {len(itens_selecionados_ca)} itens enviados! Vá para a aba 'Ordem de Compra'.")
                except Exception as e:
                    st.error(f"Erro ao enviar itens para a cesta: {e}")


# --- TAB 3: GERAÇÃO DA ORDEM DE COMPRA ---
with tab_oc:
    ordem_compra.display_oc_interface(st.session_state.get("resultado_compra", {}).get("df")) 

# --- TAB 4: GERENCIADOR DE OCS ---
with tab_gerenciador:
    gerenciador_oc.display_oc_manager()