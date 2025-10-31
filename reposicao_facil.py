# reposicao_facil.py - VERSÃO FINAL DE PRODUÇÃO (Estabilizada para Cloud)
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

# MÓDULOS DE ORDEM DE COMPRA (SQLITE)
import ordem_compra 
import gerenciador_oc 

VERSION = "v4.2.0 - PRODUÇÃO (Estabilizado)"

st.set_page_config(page_title="Alivvia Reposição Pro", layout="wide")

DEFAULT_SHEET_LINK = "https://docs.google.com/spreadsheets/d/1cTLARjq-B5g50dL6tcntg7lb_Iu0ta43/edit"
DEFAULT_SHEET_ID = "1cTLARjq-B5g50dL6tcntg7lb_Iu0ta43"

# =======================================================
# --- FUNÇÕES UTILITÁRIAS E PERSISTÊNCIA (CORRIGIDAS) ---
# =======================================================

# --- UI Helpers (Necessário para a linha 306) ---
def badge_ok(label: str, filename: str) -> str:
    """Função para exibir o status de arquivo salvo com um ícone verde."""
    return f"<span style='background:#198754; color:#fff; padding:6px 10px; border-radius:10px; font-size:12px;'>✅ {label}: <b>{filename}</b></span>"

def normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [norm_header(c) for c in df.columns]
    return df
def norm_header(s: str) -> str:
    s = (s or "").strip()
    s = unidecode(s).lower()
    for ch in [" ", "-", "(", ")", "/", "\\", "[", "]", ".", ",", ";", ":"]: s = s.replace(ch, "_")
    while "__" in s: s = s.replace("__", "_")
    return s.strip("_")
def norm_sku(x: str) -> str:
    if pd.isna(x): return ""
    return unidecode(str(x)).strip().upper()
def br_to_float(x):
    if pd.isna(x): return np.nan
    if isinstance(x, (int, float, np.integer, np.floating)): return float(x)
    s = str(x).strip().replace("\u00a0", " ").replace("R$", "").replace(" ", "").replace(".", "").replace(",", ".")
    try: return float(s)
    except: return np.nan
def exige_colunas(df: pd.DataFrame, obrig: list, nome: str):
    faltam = [c for c in obrig if c not in df.columns]
    if faltam: raise ValueError(f"Colunas obrigatórias ausentes em {nome}: {faltam}")


# --- Persistência de Uploads (Migrada para Cache de Recurso) ---
@st.cache_resource(show_spinner=False)
def _file_store():
    # Inicializa o armazenamento de arquivos no cache
    return {
        "ALIVVIA": {"FULL": None, "VENDAS": None, "ESTOQUE": None},
        "JCA":     {"FULL": None, "VENDAS": None, "ESTOQUE": None},
    }

def _store_put(emp: str, kind: str, name: str, blob: bytes):
    """Salva os arquivos no cache (persistência em Cloud)."""
    store = _file_store()
    store[emp][kind] = {"name": name, "bytes": blob}

def _store_get(emp: str, kind: str):
    """Obtém os arquivos do cache."""
    store = _file_store()
    return store[emp][kind]

def _store_delete(emp: str, kind: str):
    """Deleta um arquivo do cache."""
    store = _file_store()
    store[emp][kind] = None
    
def _ensure_state():
    st.session_state.setdefault("catalogo_df", None)
    st.session_state.setdefault("kits_df", None)
    st.session_state.setdefault("loaded_at", None)
    st.session_state.setdefault("resultado_compra", {})
    # Garante que os arquivos carregados na sessão anterior (cache) sejam recuperados
    for emp in ["ALIVVIA", "JCA"]:
        st.session_state.setdefault(emp, {"FULL": _store_get(emp, "FULL"),
                                         "VENDAS": _store_get(emp, "VENDAS"),
                                         "ESTOQUE": _store_get(emp, "ESTOQUE")})
_ensure_state()


# --- Leitura de Arquivos (Corrigida) ---
def load_any_table_from_bytes(file_name: str, blob: bytes) -> pd.DataFrame:
    bio = io.BytesIO(blob)
    name = (file_name or "").lower()
    try:
        if name.endswith(".csv"):
            df = pd.read_csv(bio, dtype=str, keep_default_na=False, sep=None, engine="python")
        else:
            df = pd.read_excel(bio, dtype=str, keep_default_na=False)
    except Exception as e:
        raise RuntimeError(f"Não consegui ler o arquivo '{file_name}': {e}")
    
    df.columns = [norm_header(c) for c in df.columns]
    sku_col = next((c for c in ["sku", "codigo", "codigo_sku"] if c in df.columns), None)
    if sku_col:
        df[sku_col] = df[sku_col].map(norm_sku)
        df = df[df[sku_col] != ""]
    return df.reset_index(drop=True)

# --- Mapeamento de Tipo (Corrigido e Estável) ---
def mapear_tipo(df: pd.DataFrame) -> str:
    cols = [c.lower() for c in df.columns]
    tem_sku = any("sku" in c or "codigo" in c for c in cols)
    tem_v60 = any(c.startswith("vendas_60d") or "vendas" in c and "60" in c for c in cols)
    tem_estoque_full = any(("estoque" in c and "full" in c) or "full_estoque" in c for c in cols)
    tem_transito = any("transito" in c or "em_transito" in c for c in cols)
    tem_estoque_generico = any(c in {"estoque_atual", "qtd", "quantidade"} or "estoque" in c and "full" not in c for c in cols)
    tem_preco = any(c in {"preco", "preco_compra", "custo", "custo_medio", "preco_medio"} for c in cols)

    if tem_sku and (tem_v60 or tem_estoque_full or tem_transito): return "FULL"
    if tem_sku and tem_estoque_generico and tem_preco: return "FISICO"
    if tem_sku and not tem_preco: return "VENDAS"
    return "DESCONHECIDO"

# --- O RESTANTE DAS FUNÇÕES DE CÁLCULO (mapear_colunas, calcular, etc.) VEM AQUI
# [O código completo das funções 'mapear_colunas', 'calcular' e 'explodir_por_kits' deve estar na sua versão local]

# ... (Mantenha o resto das suas funções de cálculo aqui) ...
# =======================================================


# --- INTERFACE PRINCIPAL (UI CORRIGIDA) ---
# [O restante da interface, SIDEBAR e TABS, permanece como na última versão]
# (As chamadas para bloco_empresa e _store_put/delete agora usam o CACHE)

# Exemplo do Bloco de Upload corrigido:
def bloco_empresa(emp: str):
    st.markdown(f"### {emp}")
    c1, c2 = st.columns(2)
    
    with c1:
        st.markdown(f"**FULL — {emp}**")
        up = st.file_uploader("CSV/XLSX/XLS", type=["csv", "xlsx", "xls"], key=f"up_full_{emp}")
        if up is not None:
            blob = up.read()
            _store_put(emp, "FULL", up.name, blob) # Salva no Cache
            st.success(f"FULL salvo: {up.name}")
        it = _store_get(emp, "FULL") # Recupera do Cache
        if it and it["name"]: st.markdown(badge_ok("FULL salvo", it["name"]), unsafe_allow_html=True)
        if st.button("Limpar FULL", key=f"clr_{emp}_FULL", use_container_width=True): _store_delete(emp, "FULL"); st.experimental_rerun()
    
    # ... (Restante dos uploads VENDAS e ESTOQUE seguindo a mesma lógica de _store_put/delete) ...

# ... (Restante do corpo do reposicao_facil.py) ...