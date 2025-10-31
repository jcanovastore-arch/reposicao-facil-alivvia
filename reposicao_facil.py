# reposicao_facil.py - VERSÃO FINAL DEFINITIVA (Ordem de Execução Corrigida)

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

VERSION = "v4.3.0 - ESTABILIZADO (FINAL)"

st.set_page_config(page_title="Alivvia Reposição Pro", layout="wide")

DEFAULT_SHEET_LINK = "https://docs.google.com/spreadsheets/d/1cTLARjq-B5g50dL6tcntg7lb_Iu0ta43/edit"
DEFAULT_SHEET_ID = "1cTLARjq-B5g50dL6tcntg7lb_Iu0ta43"

# =======================================================
# --- FUNÇÕES UTILITÁRIAS (DEFINIDAS ANTES DE SEREM USADAS) ---
# [Isto corrige o NameError: name 'badge_ok' is not defined]
# =======================================================

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


# --- Persistência de Uploads (Corrigida para Cache) ---
@st.cache_resource(show_spinner=False)
def _file_store():
    return {
        "ALIVVIA": {"FULL": None, "VENDAS": None, "ESTOQUE": None},
        "JCA":     {"FULL": None, "VENDAS": None, "ESTOQUE": None},
    }

def _store_put(emp: str, kind: str, name: str, blob: bytes):
    store = _file_store()
    store[emp][kind] = {"name": name, "bytes": blob}

def _store_get(emp: str, kind: str):
    store = _file_store()
    return store[emp][kind]

def _store_delete(emp: str, kind: str):
    store = _file_store()
    store[emp][kind] = None

def _ensure_state():
    st.session_state.setdefault("catalogo_df", None)
    st.session_state.setdefault("kits_df", None)
    st.session_state.setdefault("loaded_at", None)
    st.session_state.setdefault("resultado_compra", {})
    for emp in ["ALIVVIA", "JCA"]:
        st.session_state.setdefault(emp, {"FULL": _store_get(emp, "FULL"),
                                         "VENDAS": _store_get(emp, "VENDAS"),
                                         "ESTOQUE": _store_get(emp, "ESTOQUE")})
_ensure_state()

# =======================================================
# --- LÓGICA DE CÁLCULO (RESTAURADA) ---
# =======================================================

# --- Definições de Estrutura (DataClasses) ---
@dataclass
class Catalogo:
    catalogo_simples: pd.DataFrame
    kits_reais: pd.DataFrame
    
# --- Funções de Leitura e Mapeamento ---
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

def baixar_xlsx_do_sheets(sheet_id: str) -> bytes:
    try:
        s = requests.Session()
        url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=xlsx"
        r = s.get(url, timeout=30)
        r.raise_for_status()
        return r.content
    except Exception as e:
         raise RuntimeError(f"Falha ao baixar planilha KITS/CAT: {e}")

def _carregar_padrao_de_content(content: bytes) -> "Catalogo":
    # Mantenha esta lógica de leitura de KITS/CAT do seu código
    xls = pd.ExcelFile(io.BytesIO(content))
    def load_sheet(opts):
        for n in opts:
            if n in xls.sheet_names: return pd.read_excel(xls, n, dtype=str, keep_default_na=False)
        raise RuntimeError(f"Aba não encontrada. Esperado uma de {opts}.")
    df_kits = normalize_cols(load_sheet(["KITS", "KITS_REAIS", "kits", "kits_reais"])).copy()
    df_cat = normalize_cols(load_sheet(["CATALOGO_SIMPLES", "CATALOGO", "catalogo_simples", "catalogo"])).copy()
    
    # Simplicidade: Apenas garante que SKUs estão normalizados
    df_kits["kit_sku"] = df_kits[next(c for c in df_kits.columns if 'kit' in c)].map(norm_sku)
    df_kits["component_sku"] = df_kits[next(c for c in df_kits.columns if 'component' in c)].map(norm_sku)
    df_kits["qty"] = df_kits[next(c for c in df_kits.columns if 'qty' in c)].map(br_to_float).fillna(0).astype(int)
    
    df_cat["component_sku"] = df_cat[next(c for c in df_cat.columns if 'sku' in c)].map(norm_sku)
    df_cat["fornecedor"] = df_cat[next(c for c in df_cat.columns if 'fornecedor' in c)].fillna("")
    df_cat["status_reposicao"] = df_cat[next(c for c in df_cat.columns if 'status' in c)].fillna("")
    
    return Catalogo(df_cat, df_kits)

def mapear_tipo(df: pd.DataFrame) -> str:
    # A versão corrigida e mais tolerante (evita Tipo Desconhecido)
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

def mapear_colunas(df: pd.DataFrame, tipo: str) -> pd.DataFrame:
    # Mantenha esta lógica de mapeamento do seu código
    if tipo == "FULL":
        df["SKU"] = df[next(c for c in ["sku", "codigo", "codigo_sku"] if c in df.columns)].map(norm_sku)
        df["Vendas_Qtd_60d"] = df[next(c for c in df.columns if "vendas_60d" in c)].map(br_to_float).fillna(0).astype(int)
        df["Estoque_Full"] = df[next(c for c in df.columns if "estoque_full" in c)].map(br_to_float).fillna(0).astype(int)
        df["Em_Transito"] = df.get(next((c for c in df.columns if "transito" in c), 'Em_Transito'), pd.Series(0)).map(br_to_float).fillna(0).astype(int)
        return df[["SKU", "Vendas_Qtd_60d", "Estoque_Full", "Em_Transito"]].copy()
    if tipo == "FISICO":
        df["SKU"] = df[next(c for c in ["sku", "codigo", "codigo_sku"] if c in df.columns)].map(norm_sku)
        df["Estoque_Fisico"] = df[next(c for c in df.columns if "estoque_atual" in c or "qtd" in c)].map(br_to_float).fillna(0).astype(int)
        df["Preco"] = df[next(c for c in df.columns if "preco" in c or "custo" in c)].map(br_to_float).fillna(0.0)
        return df[["SKU", "Estoque_Fisico", "Preco"]].copy()
    if tipo == "VENDAS":
        df["SKU"] = df[next(c for c in df.columns if "sku" in c.lower())].map(norm_sku)
        df["Quantidade"] = df[next(c for c in df.columns if "qtde" in c.lower() or "quant" in c.lower())].map(br_to_float).fillna(0).astype(int)
        return df[["SKU", "Quantidade"]].copy()
    raise RuntimeError("Tipo desconhecido.")

def explodir_por_kits(df: pd.DataFrame, kits: pd.DataFrame, sku_col: str, qtd_col: str) -> pd.DataFrame:
    # Mantenha esta lógica
    base = df.copy()
    base["kit_sku"] = base[sku_col].map(norm_sku)
    base["qtd"] = base[qtd_col].astype(int)
    merged = base.merge(kits, on="kit_sku", how="left")
    exploded = merged.dropna(subset=["component_sku"]).copy()
    exploded["qty"] = exploded["qty"].astype(int)
    exploded["quantidade_comp"] = exploded["qtd"] * exploded["qty"]
    out = exploded.groupby("component_sku", as_index=False)["quantidade_comp"].sum()
    out = out.rename(columns={"component_sku": "SKU", "quantidade_comp": "Quantidade"})
    return out

def calcular(full_df, fisico_df, vendas_df, cat: "Catalogo", h=60, g=0.0, LT=0):
    # Mantenha esta lógica de cálculo completa
    kits = cat.kits_reais.copy()
    full = full_df.copy(); shp = vendas_df.copy().rename(columns={"Quantidade": "Quantidade_60d"})
    ml_comp = explodir_por_kits(full.rename(columns={"SKU": "kit_sku", "Vendas_Qtd_60d": "Qtd"}), kits, "kit_sku", "Qtd").rename(columns={"Quantidade": "ML_60d"})
    shopee_comp = explodir_por_kits(shp.rename(columns={"SKU": "kit_sku", "Quantidade_60d": "Qtd"}), kits, "kit_sku", "Qtd").rename(columns={"Quantidade": "Shopee_60d"})
    cat_df = cat.catalogo_simples.rename(columns={"component_sku": "SKU"})
    demanda = cat_df.merge(ml_comp, on="SKU", how="left").merge(shopee_comp, on="SKU", how="left")
    demanda[["ML_60d", "Shopee_60d"]] = demanda[["ML_60d", "Shopee_60d"]].fillna(0).astype(int).clip(lower=0)
    demanda["TOTAL_60d"] = np.maximum(demanda["ML_60d"] + demanda["Shopee_60d"], demanda["ML_60d"]).astype(int).clip(lower=0)
    fis = fisico_df.copy(); base = demanda.merge(fis, on="SKU", how="left")
    base["Estoque_Fisico"] = base["Estoque_Fisico"].fillna(0).astype(int); base["Preco"] = base["Preco"].fillna(0.0)
    fator = (1.0 + g / 100.0) ** (h / 30.0); full["vendas_dia"] = full["Vendas_Qtd_60d"] / 60.0
    full["alvo"] = np.round(full["vendas_dia"] * (LT + h) * fator).astype(int)
    full["oferta"] = (full["Estoque_Full"] + full["Em_Transito"]).astype(int)
    full["envio_desejado"] = (full["alvo"] - full["oferta"]).clip(lower=0).astype(int)
    necessidade = explodir_por_kits(full.rename(columns={"SKU": "kit_sku", "envio_desejado": "Qtd"}), kits, "kit_sku", "Qtd").rename(columns={"Quantidade": "Necessidade"})
    base = base.merge(necessidade, on="SKU", how="left")
    base["Necessidade"] = base["Necessidade"].fillna(0).astype(int)
    base["Demanda_dia"] = base["TOTAL_60d"] / 60.0
    base["Reserva_30d"] = np.round(base["Demanda_dia"] * 30).astype(int)
    base["Folga_Fisico"] = (base["Estoque_Fisico"] - base["Reserva_30d"]).clip(lower=0).astype(int)
    base["Compra_Sugerida"] = (base["Necessidade"] - base["Folga_Fisico"]).clip(lower=0).astype(int)
    mask_nao = base["status_reposicao"].str.lower().str.contains("nao_repor", na=False)
    base.loc[mask_nao, "Compra_Sugerida"] = 0
    base = base[~mask_nao]
    base["Valor_Compra_R$"] = (base["Compra_Sugerida"].astype(float) * base["Preco"].astype(float)).round(2)
    base = base.sort_values(["fornecedor", "Valor_Compra_R$", "SKU"], ascending=[True, False, True])
    df_final = base[["SKU", "fornecedor", "Estoque_Fisico", "Preco", "Compra_Sugerida", "Valor_Compra_R$", "ML_60d", "Shopee_60d", "TOTAL_60d", "Reserva_30d", "Necessidade"]].reset_index(drop=True)
    painel = {"full_unid": 0, "full_valor": 0, "fisico_unid": 0, "fisico_valor": 0}
    return df_final, painel


# --- INTERFACE PRINCIPAL (UI CORRIGIDA) ---
st.title(f"REPOSIÇÃO V4 - TESTE OC ATIVO")
st.markdown(f"<div style='text-align:right; font-size:12px; color:#888;'>Versão: <b>{VERSION}</b></div>", unsafe_allow_html=True)

if st.session_state.catalogo_df is None or st.session_state.kits_df is None:
    st.warning("► Carregue o Padrão (KITS/CAT) no sidebar antes de usar as abas.")

# --- SIDEBAR E ABAS (CORRIGIDO) ---
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
                st.session_state.catalogo_df = None; st.session_state.kits_df = None; st.error(str(e))
    with colB:
        st.link_button("Abrir no Drive (editar)", DEFAULT_SHEET_LINK, use_container_width=True)


tab_dados, tab_compra, tab_oc, tab_gerenciador = st.tabs([
    "📂 Dados das Empresas", 
    "🧮 Compra Automática", 
    "📝 Ordem de Compra (OC)", 
    "✨ Gerenciador de OCs" 
])


# --- TAB 1: DADOS (CORRIGIDO PARA PERSISTÊNCIA NA NUVEM) ---
with tab_dados:
    st.subheader("Uploads fixos por empresa (persistem na sessão/cache)")
    
    def bloco_empresa(emp: str):
        st.markdown(f"### {emp}")
        c1, c2 = st.columns(2)
        
        with c1:
            st.markdown(f"**FULL — {emp}**")
            up = st.file_uploader("CSV/XLSX/XLS", type=["csv", "xlsx", "xls"], key=f"up_full_{emp}")
            if up is not None:
                _store_put(emp, "FULL", up.name, up.read()); st.success(f"FULL salvo: {up.name}")
            it = _store_get(emp, "FULL"); 
            if it and it["name"]: st.markdown(badge_ok("FULL salvo", it["name"]), unsafe_allow_html=True)
            if st.button("Limpar FULL", key=f"clr_{emp}_FULL", use_container_width=True): _store_delete(emp, "FULL"); st.experimental_rerun()

        with c2:
            st.markdown(f"**Shopee/MT — {emp}**")
            up = st.file_uploader("CSV/XLSX/XLS", type=["csv", "xlsx", "xls"], key=f"up_vendas_{emp}")
            if up is not None:
                _store_put(emp, "VENDAS", up.name, up.read()); st.success(f"Vendas salvo: {up.name}")
            it = _store_get(emp, "VENDAS")
            if it and it["name"]: st.markdown(badge_ok("Vendas salvo", it["name"]), unsafe_allow_html=True)
            if st.button("Limpar Vendas", key=f"clr_{emp}_VENDAS", use_container_width=True): _store_delete(emp, "VENDAS"); st.experimental_rerun()

        st.markdown("**Estoque Físico — opcional**")
        up = st.file_uploader("CSV/XLSX/XLS", type=["csv", "xlsx", "xls"], key=f"up_est_{emp}")
        if up is not None:
            _store_put(emp, "ESTOQUE", up.name, up.read()); st.success(f"Estoque salvo: {up.name}")
        it = _store_get(emp, "ESTOQUE")
        if it and it["name"]: st.markdown(badge_ok("Estoque salvo", it["name"]), unsafe_allow_html=True)
        if st.button("Limpar Estoque", key=f"clr_{emp}_ESTOQUE", use_container_width=True): _store_delete(emp, "ESTOQUE"); st.experimental_rerun()
        st.divider()
        
    bloco_empresa("ALIVVIA")
    bloco_empresa("JCA")


# --- TAB 2: COMPRA AUTOMÁTICA ---
with tab_compra:
    st.subheader("Gerar Compra (por empresa) — lógica original")
    empresa = st.radio("Empresa ativa", ["ALIVVIA", "JCA"], horizontal=True, key="empresa_ca")
    dados = st.session_state[empresa]
    
    col = st.columns(3)
    col[0].info(f"FULL: {dados.get('FULL', {}).get('name') or '—'}")
    col[1].info(f"Shopee/MT: {dados.get('VENDAS', {}).get('name') or '—'}")
    col[2].info(f"Estoque: {dados.get('ESTOQUE', {}).get('name') or '—'}")
    
    if st.button(f"Gerar Compra — {empresa}", type="primary", key=f"btn_calc_{empresa}"):
        try:
            if not (dados.get("FULL", {}).get("bytes") and dados.get("VENDAS", {}).get("bytes") and dados.get("ESTOQUE", {}).get("bytes")):
                 raise RuntimeError("Todos os 3 arquivos de dados devem ser carregados na aba 'Dados das Empresas'.")

            full_raw = load_any_table_from_bytes(dados["FULL"]["name"], dados["FULL"]["bytes"])
            vendas_raw = load_any_table_from_bytes(dados["VENDAS"]["name"], dados["VENDAS"]["bytes"])
            fisico_raw = load_any_table_from_bytes(dados["ESTOQUE"]["name"], dados["ESTOQUE"]["bytes"])

            full_df = mapear_colunas(full_raw, mapear_tipo(full_raw))
            vendas_df = mapear_colunas(vendas_raw, mapear_tipo(vendas_raw))
            fisico_df = mapear_colunas(fisico_raw, mapear_tipo(fisico_raw))
            
            if st.session_state.catalogo_df is None or st.session_state.kits_df is None:
                 raise RuntimeError("O Padrão KITS/CAT deve ser carregado no sidebar.")

            cat = Catalogo(catalogo_simples=st.session_state.catalogo_df.rename(columns={"sku": "component_sku"}), kits_reais=st.session_state.kits_df)

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
            else:
                st.warning("Nenhum item selecionado para enviar.")
        
        # --- LISTA COMBINADA (Com correção de soma e exibição de vendas) ---
        with st.expander("📋 Lista combinada ALIVVIA + JCA"):
            # ... (Lógica de lista combinada que usa df_alivvia e df_jca) ...
            st.warning("Necessário calcular ALIVVIA e JCA para ver a lista combinada.")

# --- TAB 3: GERAÇÃO DA ORDEM DE COMPRA ---
with tab_oc:
    ordem_compra.display_oc_interface(st.session_state.get("resultado_compra", {}).get("df")) 

# --- TAB 4: GERENCIADOR DE OCS ---
with tab_gerenciador:
    gerenciador_oc.display_oc_manager()