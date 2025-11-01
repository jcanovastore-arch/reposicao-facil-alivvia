# reposicao_facil.py - VERSÃO FINAL DE PRODUÇÃO (V4.23.0 - PATCH COMPLETO)
# - FIX ESTRATÉGICO: Compra Automática mantém dados fixos e ganha filtros dinâmicos.
# - FIX ESTRATÉGICO: Compra Conjunta simplificada (sem Em_Transito e Folga_Fisico).
# - FIX: Botão 'Enviar para OC' 100% garantido.

import io
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

# MÓDULOS DE ORDEM DE COMPRA (SQLITE) - NECESSÁRIOS PARA AS ABAS OC/GERENCIADOR
try:
    import ordem_compra 
    import gerenciador_oc 
except ImportError:
    pass 

VERSION = "v4.23.0 - PATCH COMPLETO"

# ===================== CONFIG BÁSICA =====================
st.set_page_config(page_title="Reposição Logística — Alivvia", layout="wide")

DEFAULT_SHEET_LINK = (
    "https://docs.google.com/spreadsheets/d/1cTLARjq-B5g50dL6tcntg7lb_Iu0ta43/"
    "edit?usp=sharing&ouid=109458533144345974874&rtpof=true&sd=true"
)
DEFAULT_SHEET_ID = "1cTLARjq-B5g50dL6tcntg7lb_Iu0ta43"  # ID fixo

# ===================== ESTADO =====================
def _ensure_state():
    st.session_state.setdefault("catalogo_df", None)
    st.session_state.setdefault("kits_df", None)
    st.session_state.setdefault("loaded_at", None)
    st.session_state.setdefault("alt_sheet_link", DEFAULT_SHEET_LINK)
    st.session_state.setdefault("oc_cesta", pd.DataFrame()) 
    
    # Estado para manter os resultados da Compra Automática FIXOS
    st.session_state.setdefault("compra_autom_data", {})

    # uploads por empresa
    for emp in ["ALIVVIA", "JCA"]:
        st.session_state.setdefault(emp, {})
        st.session_state[emp].setdefault("FULL",   {"name": None, "bytes": None})
        st.session_state[emp].setdefault("VENDAS", {"name": None, "bytes": None})
        st.session_state[emp].setdefault("ESTOQUE",{"name": None, "bytes": None})

_ensure_state()

# ===================== HTTP / GOOGLE SHEETS =====================
def _requests_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(total=3, backoff_factor=0.6, status_forcelist=[429,500,502,503,504], allowed_methods=["GET"])
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.headers.update({"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125 Safari/537.36"})
    return s

def gs_export_xlsx_url(sheet_id: str) -> str:
    return f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=xlsx"

def extract_sheet_id_from_url(url: str) -> Optional[str]:
    if not url: return None
    m = re.search(r"/d/([a-zA-Z0-9\-_]+)/", url)
    return m.group(1) if m else None

def baixar_xlsx_por_link_google(url: str) -> bytes:
    s = _requests_session()
    if "export?format=xlsx" in url:
        r = s.get(url, timeout=30); r.raise_for_status(); return r.content
    sid = extract_sheet_id_from_url(url)
    if not sid: raise RuntimeError("Link inválido do Google Sheets (esperado .../d/<ID>/...).")
    r = s.get(gs_export_xlsx_url(sid), timeout=30); r.raise_for_status(); return r.content

# ADICIONANDO CACHE PARA ESTABILIDADE NO STREAMLIT CLOUD
@st.cache_data(show_spinner="Baixando Planilha de Padrões KITS/CAT...")
def baixar_xlsx_do_sheets(sheet_id: str) -> bytes:
    s = _requests_session()
    url = gs_export_xlsx_url(sheet_id)
    try:
        r = s.get(url, timeout=30)
        r.raise_for_status()
        return r.content
    except requests.HTTPError as e:
        sc = getattr(e.response, "status_code", "?")
        raise RuntimeError(
            f"Falha ao baixar XLSX (HTTP {sc}). Verifique: compartilhamento 'Qualquer pessoa com link – Leitor'.\nURL: {url}"
        )
    except Exception as e:
        raise RuntimeError(f"Falha crítica no download: {e}")

# ===================== UTILS DE DADOS =====================
def norm_header(s: str) -> str:
    s = (s or "").strip()
    s = unidecode(s).lower()
    for ch in [" ", "-", "(", ")", "/", "\\", "[", "]", ".", ",", ";", ":"]:
        s = s.replace(ch, "_")
    while "__" in s:
        s = s.replace("__", "_")
    return s.strip("_")

def normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [norm_header(c) for c in df.columns]
    return df

def br_to_float(x):
    if pd.isna(x): return np.nan
    if isinstance(x,(int,float,np.integer,np.floating)): return float(x)
    s = str(x).strip()
    if s == "": return np.nan
    s = s.replace("\u00a0"," ").replace("R$","").replace(" ","").replace(".","").replace(",",".")
    try: return float(s)
    except: return np.nan

def norm_sku(x: str) -> str:
    if pd.isna(x): return ""
    return unidecode(str(x)).strip().upper()

def exige_colunas(df: pd.DataFrame, obrig: list, nome: str):
    faltam = [c for c in obrig if c not in df.columns]
    if faltam:
        raise ValueError(f"Colunas obrigatórias ausentes em {nome}: {faltam}\nColunas lidas: {list(df.columns)}")

# ===================== LEITURA DE ARQUIVOS =====================

def load_any_table_from_bytes(file_name: str, blob: bytes) -> pd.DataFrame:
    """Leitura a partir de bytes salvos na sessão (com fallback header=2)."""
    bio = io.BytesIO(blob); name = (file_name or "").lower()
    try:
        if name.endswith(".csv"):
            df = pd.read_csv(bio, dtype=str, keep_default_na=False, sep=None, engine="python")
        else:
            df = pd.read_excel(bio, dtype=str, keep_default_na=False)
    except Exception as e:
        raise RuntimeError(f"Não consegui ler o arquivo salvo '{file_name}': {e}")

    df.columns = [norm_header(c) for c in df.columns]
    tem_col_sku = any(c in df.columns for c in ["sku","codigo","codigo_sku"]) or any("sku" in c for c in df.columns)
    if (not tem_col_sku) and (len(df) > 0):
        try:
            bio.seek(0)
            if name.endswith(".csv"):
                df = pd.read_csv(bio, dtype=str, keep_default_na=False, header=2)
            else:
                df = pd.read_excel(bio, dtype=str, keep_default_na=False, header=2)
            df.columns = [norm_header(c) for c in df.columns]
        except Exception:
            pass

    cols = set(df.columns)
    sku_col = next((c for c in ["sku","codigo","codigo_sku"] if c in cols), None)
    if sku_col:
        df[sku_col] = df[sku_col].map(norm_sku)
        df = df[df[sku_col] != ""]
    for c in list(df.columns):
        df = df[~df[c].astype(str).str.contains(r"^TOTALS?$|^TOTAIS?$", case=False, na=False)]
    return df.reset_index(drop=True)

# ===================== PADRÃO KITS/CAT =====================
@dataclass
class Catalogo:
    catalogo_simples: pd.DataFrame  # component_sku, fornecedor, status_reposicao
    kits_reais: pd.DataFrame        # kit_sku, component_sku, qty

def _carregar_padrao_de_content(content: bytes) -> Catalogo:
    try:
        xls = pd.ExcelFile(io.BytesIO(content))
    except Exception as e:
        raise RuntimeError(f"Arquivo XLSX inválido: {e}")

    def load_sheet(opts):
        for n in opts:
            if n in xls.sheet_names:
                return pd.read_excel(xls, n, dtype=str, keep_default_na=False)
        raise RuntimeError(f"Aba não encontrada. Esperado uma de {opts}. Abas: {xls.sheet_names}")

    df_kits = load_sheet(["KITS","KITS_REAIS","kits","kits_reais"]).copy()
    df_cat  = load_sheet(["CATALOGO_SIMPLES","CATALOGO","catalogo_simples","catalogo"]).copy()

    # KITS: lógica de normalização (mantida)
    df_kits = normalize_cols(df_kits)
    possiveis_kits = {
        "kit_sku": ["kit_sku", "kit", "sku_kit"],
        "component_sku": ["component_sku","componente","sku_componente","component","sku_component"],
        "qty": ["qty","qty_por_kit","qtd_por_kit","quantidade_por_kit","qtd","quantidade"]
    }
    rename_k = {}
    for alvo, cand in possiveis_kits.items():
        for c in cand:
            if c in df_kits.columns:
                rename_k[c] = alvo; break
    df_kits = df_kits.rename(columns=rename_k)
    exige_colunas(df_kits, ["kit_sku","component_sku","qty"], "KITS")
    df_kits = df_kits[["kit_sku","component_sku","qty"]].copy()
    df_kits["kit_sku"] = df_kits["kit_sku"].map(norm_sku)
    df_kits["component_sku"] = df_kits["component_sku"].map(norm_sku)
    df_kits["qty"] = df_kits["qty"].map(br_to_float).fillna(0).astype(int)
    df_kits = df_kits[df_kits["qty"] >= 1].drop_duplicates(subset=["kit_sku","component_sku"], keep="first")

    # CATALOGO: lógica de normalização (mantida)
    df_cat = normalize_cols(df_cat)
    possiveis_cat = {
        "component_sku": ["component_sku","sku","produto","item","codigo","sku_componente"],
        "fornecedor": ["fornecedor","supplier","fab","marca"],
        "status_reposicao": ["status_reposicao","status","reposicao_status"]
    }
    rename_c = {}
    for alvo, cand in possiveis_cat.items():
        for c in cand:
            if c in df_cat.columns:
                rename_c[c] = alvo; break
    df_cat = df_cat.rename(columns=rename_c)
    if "component_sku" not in df_cat.columns:
        raise ValueError("CATALOGO precisa ter a coluna 'component_sku' (ou 'sku').")
    if "fornecedor" not in df_cat.columns:
        df_cat["fornecedor"] = ""
    if "status_reposicao" not in df_cat.columns:
        df_cat["status_reposicao"] = ""
    df_cat["component_sku"] = df_cat["component_sku"].map(norm_sku)
    df_cat["fornecedor"] = df_cat["fornecedor"].fillna("").astype(str)
    df_cat["status_reposicao"] = df_cat["status_reposicao"].fillna("").astype(str)
    df_cat = df_cat.drop_duplicates(subset=["component_sku"], keep="last")

    return Catalogo(catalogo_simples=df_cat, kits_reais=df_kits)

def carregar_padrao_do_xlsx(sheet_id: str) -> Catalogo:
    content = baixar_xlsx_do_sheets(sheet_id)
    return _carregar_padrao_de_content(content)

def carregar_padrao_do_link(url: str) -> Catalogo:
    content = baixar_xlsx_por_link_google(url)
    return _carregar_padrao_de_content(content)

def construir_kits_efetivo(cat: Catalogo) -> pd.DataFrame:
    kits = cat.kits_reais.copy()
    existentes = set(kits["kit_sku"].unique())
    alias = []
    for s in cat.catalogo_simples["component_sku"].unique().tolist():
        s = norm_sku(s)
        if s and s not in existentes:
            alias.append((s, s, 1))
    if alias:
        kits = pd.concat([kits, pd.DataFrame(alias, columns=["kit_sku","component_sku","qty"])], ignore_index=True)
    kits = kits.drop_duplicates(subset=["kit_sku","component_sku"], keep="first")
    return kits

# ===================== MAPEAMENTO E COLUNAS (sem alteração) =====================
def mapear_tipo(df: pd.DataFrame) -> str:
    cols = [c.lower() for c in df.columns]
    tem_sku_std  = any(c in {"sku","codigo","codigo_sku"} for c in cols) or any("sku" in c for c in cols)
    tem_vendas60 = any(c.startswith("vendas_60d") or c in {"vendas 60d","vendas_qtd_60d"} for c in cols)
    tem_qtd_livre= any(("qtde" in c) or ("quant" in c) or ("venda" in c) or ("order" in c) for c in cols)
    tem_estoque_full_like = any(("estoque" in c and "full" in c) or c=="estoque_full" for c in cols)
    tem_estoque_generico  = any(c in {"estoque_atual","qtd","quantidade"} or "estoque" in c for c in cols)
    tem_transito_like     = any(("transito" in c) or c in {"em_transito","em transito","em_transito_full","em_transito_do_anuncio"} for c in cols)
    tem_preco = any(c in {"preco","preco_compra","preco_medio","custo","custo_medio"} for c in cols)

    if tem_sku_std and (tem_vendas60 or tem_estoque_full_like or tem_transito_like):
        return "FULL"
    if tem_sku_std and tem_estoque_generico and tem_preco:
        return "FISICO"
    if tem_sku_std and tem_qtd_livre and not tem_preco:
        return "VENDAS"
    return "DESCONHECIDO"

def mapear_colunas(df: pd.DataFrame, tipo: str) -> pd.DataFrame:
    if tipo == "FULL":
        if "sku" in df.columns:           df["SKU"] = df["sku"].map(norm_sku)
        elif "codigo" in df.columns:      df["SKU"] = df["codigo"].map(norm_sku)
        elif "codigo_sku" in df.columns:  df["SKU"] = df["codigo_sku"].map(norm_sku)
        else: raise RuntimeError("FULL inválido: precisa de coluna SKU/codigo.")

        c_v = [c for c in df.columns if c in ["vendas_qtd_60d","vendas_60d","vendas 60d"] or c.startswith("vendas_60d")]
        if not c_v: raise RuntimeError("FULL inválido: faltou Vendas_60d.")
        df["Vendas_Qtd_60d"] = df[c_v[0]].map(br_to_float).fillna(0).astype(int)

        c_e = [c for c in df.columns if c in ["estoque_full","estoque_atual"] or ("estoque" in c and "full" in c)]
        if not c_e: raise RuntimeError("FULL inválido: faltou Estoque_Full/estoque_atual.")
        df["Estoque_Full"] = df[c_e[0]].map(br_to_float).fillna(0).astype(int)

        c_t = [c for c in df.columns if c in ["em_transito","em transito","em_transito_full","em_transito_do_anuncio"] or ("transito" in c)]
        # INSTRUÇÃO: Em_Transito é mantido aqui, mas removido do df_final em calcular()
        df["Em_Transito"] = df[c_t[0]].map(br_to_float).fillna(0).astype(int) if c_t else 0

        return df[["SKU","Vendas_Qtd_60d","Estoque_Full","Em_Transito"]].copy()

    if tipo == "FISICO":
        sku_series = (
            df["sku"] if "sku" in df.columns else
            (df["codigo"] if "codigo" in df.columns else
             (df["codigo_sku"] if "codigo_sku" in df.columns else None))
        )
        if sku_series is None:
            cand = next((c for c in df.columns if "sku" in c.lower()), None)
            if cand is None: raise RuntimeError("FÍSICO inválido: não achei coluna de SKU.")
            sku_series = df[cand]
        df["SKU"] = sku_series.map(norm_sku)

        c_q = [c for c in df.columns if c in ["estoque_atual","qtd","quantidade"] or ("estoque" in c)]
        if not c_q: raise RuntimeError("FÍSICO inválido: faltou Estoque.")
        df["Estoque_Fisico"] = df[c_q[0]].map(br_to_float).fillna(0).astype(int)

        c_p = [c for c in df.columns if c in ["preco","preco_compra","custo","custo_medio","preco_medio","preco_unitario"]]
        if not c_p: raise RuntimeError("FÍSICO inválido: faltou Preço/Custo.")
        df["Preco"] = df[c_p[0]].map(br_to_float).fillna(0.0)

        return df[["SKU","Estoque_Fisico","Preco"]].copy()

    if tipo == "VENDAS":
        sku_col = next((c for c in df.columns if "sku" in c.lower()), None)
        if sku_col is None:
            raise RuntimeError("VENDAS inválido: não achei coluna de SKU.")
        df["SKU"] = df[sku_col].map(norm_sku)

        cand_qty = []
        for c in df.columns:
            cl = c.lower(); score = 0
            if "qtde" in cl: score += 3
            if "quant" in cl: score += 2
            if "venda" in cl: score += 1
            if "order" in cl: score += 1
            if score > 0: cand_qty.append((score, c))
        if not cand_qty:
            raise RuntimeError("VENDAS inválido: não achei coluna de Quantidade.")
        cand_qty.sort(reverse=True)
        qcol = cand_qty[0][1]
        df["Quantidade"] = df[qcol].map(br_to_float).fillna(0).astype(int)
        return df[["SKU","Quantidade"]].copy()

    raise RuntimeError("Tipo de arquivo desconhecido.")
    
def explodir_por_kits(df: pd.DataFrame, kits: pd.DataFrame, sku_col: str, qtd_col: str) -> pd.DataFrame:
    base = df.copy()
    base["kit_sku"] = base[sku_col].map(norm_sku)
    base["qtd"]     = base[qtd_col].astype(int)
    merged   = base.merge(kits, on="kit_sku", how="left")
    exploded = merged.dropna(subset=["component_sku"]).copy()
    exploded["qty"] = exploded["qty"].astype(int)
    exploded["quantidade_comp"] = exploded["qtd"] * exploded["qty"]
    out = exploded.groupby("component_sku", as_index=False)["quantidade_comp"].sum()
    out = out.rename(columns={"component_sku":"SKU","quantidade_comp":"Quantidade"})
    return out

# ===================== CÁLCULOS PRINCIPAIS (com remoção de colunas) =====================
def calcular(full_df, fisico_df, vendas_df, cat: Catalogo, h=60, g=0.0, LT=0):
    kits = construir_kits_efetivo(cat)
    full = full_df.copy()
    full["SKU"] = full["SKU"].map(norm_sku)
    full["Vendas_Qtd_60d"] = full["Vendas_Qtd_60d"].astype(int)
    full["Estoque_Full"]   = full["Estoque_Full"].astype(int)
    full["Em_Transito"]    = full["Em_Transito"].astype(int)

    shp = vendas_df.copy()
    shp["SKU"] = shp["SKU"].map(norm_sku)
    shp["Quantidade_60d"] = shp["Quantidade"].astype(int)

    ml_comp = explodir_por_kits(
        full[["SKU","Vendas_Qtd_60d"]].rename(columns={"SKU":"kit_sku","Vendas_Qtd_60d":"Qtd"}),
        kits,"kit_sku","Qtd").rename(columns={"Quantidade":"ML_60d"})
    shopee_comp = explodir_por_kits(
        shp[["SKU","Quantidade_60d"]].rename(columns={"SKU":"kit_sku","Quantidade_60d":"Qtd"}),
        kits,"kit_sku","Qtd").rename(columns={"Quantidade":"Shopee_60d"})

    cat_df = cat.catalogo_simples[["component_sku","fornecedor","status_reposicao"]].rename(columns={"component_sku":"SKU"})

    demanda = cat_df.merge(ml_comp, on="SKU", how="left").merge(shopee_comp, on="SKU", how="left")
    demanda[["ML_60d","Shopee_60d"]] = demanda[["ML_60d","Shopee_60d"]].fillna(0).astype(int)
    demanda["TOTAL_60d"] = np.maximum(demanda["ML_60d"] + demanda["Shopee_60d"], demanda["ML_60d"]).astype(int)

    fis = fisico_df.copy()
    fis["SKU"] = fis["SKU"].map(norm_sku)
    fis["Estoque_Fisico"] = fis["Estoque_Fisico"].fillna(0).astype(int)
    fis["Preco"] = fis["Preco"].fillna(0.0)

    base = demanda.merge(fis, on="SKU", how="left")
    base["Estoque_Fisico"] = base["Estoque_Fisico"].fillna(0).astype(int)
    base["Preco"] = base["Preco"].fillna(0.0)

    fator = (1.0 + g/100.0) ** (h/30.0)
    fk = full.copy()
    fk["vendas_dia"] = fk["Vendas_Qtd_60d"] / 60.0
    fk["alvo"] = np.round(fk["vendas_dia"] * (LT + h) * fator).astype(int)
    # Lógica de oferta usa Em_Transito/Estoque_Full (correto)
    fk["oferta"] = (fk["Estoque_Full"] + fk["Em_Transito"]).astype(int)
    fk["envio_desejado"] = (fk["alvo"] - fk["oferta"]).clip(lower=0).astype(int)

    necessidade = explodir_por_kits(
        fk[["SKU","envio_desejado"]].rename(columns={"SKU":"kit_sku","envio_desejado":"Qtd"}),
        kits,"kit_sku","Qtd").rename(columns={"Quantidade":"Necessidade"})

    base = base.merge(necessidade, on="SKU", how="left")
    base["Necessidade"] = base["Necessidade"].fillna(0).astype(int)

    base["Demanda_dia"]  = base["TOTAL_60d"] / 60.0
    base["Reserva_30d"]  = np.round(base["Demanda_dia"] * 30).astype(int)
    
    # Folga_Fisico é calculado para o cálculo, mas será removido na saída
    base["Folga_Fisico"] = (base["Estoque_Fisico"] - base["Reserva_30d"]).clip(lower=0).astype(int)

    base["Compra_Sugerida"] = (base["Necessidade"] - base["Folga_Fisico"]).clip(lower=0).astype(int)

    mask_nao = base["status_reposicao"].str.lower().str.contains("nao_repor", na=False)
    base.loc[mask_nao, "Compra_Sugerida"] = 0

    base["Valor_Compra_R$"] = (base["Compra_Sugerida"].astype(float) * base["Preco"].astype(float)).round(2)
    base["Vendas_h_ML"]     = np.round(base["ML_60d"] * (h/60.0)).astype(int)
    base["Vendas_h_Shopee"] = np.round(base["Shopee_60d"] * (h/60.0)).astype(int)

    base = base.sort_values(["fornecedor","Valor_Compra_R$","SKU"], ascending=[True, False, True])

    # COLUNAS FINAIS (Simplificadas - Removendo Em_Transito, Reserva_30d, Folga_Fisico, Necessidade)
    df_final = base[[
        "SKU","fornecedor",
        # Removido: "Vendas_h_ML","Vendas_h_Shopee", (Usaremos o ML_60d/Shopee_60d)
        "Estoque_Fisico","Preco","Compra_Sugerida","Valor_Compra_R$",
        "ML_60d","Shopee_60d","TOTAL_60d"
        # Removido: "Reserva_30d","Folga_Fisico","Necessidade"
    ]].reset_index(drop=True)

    # Painel (mantido)
    fis_unid  = int(fis["Estoque_Fisico"].sum())
    fis_valor = float((fis["Estoque_Fisico"] * fis["Preco"]).sum())
    full_stock_comp = explodir_por_kits(
        full[["SKU","Estoque_Full"]].rename(columns={"SKU":"kit_sku","Estoque_Full":"Qtd"}),
        kits,"kit_sku","Qtd")
    full_stock_comp = full_stock_comp.merge(fis[["SKU","Preco"]], on="SKU", how="left")
    full_unid  = int(full["Estoque_Full"].sum())
    full_valor = float((full_stock_comp["Quantidade"].fillna(0) * full_stock_comp["Preco"].fillna(0.0)).sum())

    painel = {"full_unid": full_unid, "full_valor": full_valor, "fisico_unid": fis_unid, "fisico_valor": fis_valor}
    return df_final, painel

# ===================== FUNÇÕES DE AGREGAÇÃO PARA COMPRA CONJUNTA =====================
def _aggregate_data_for_conjunta(emp_a="ALIVVIA", emp_j="JCA") -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Lê e agrega dados FULL, VENDAS e FÍSICO das duas empresas."""
    
    def read_and_map(emp: str):
        full_r = load_any_table_from_bytes(st.session_state[emp]["FULL"]["name"], st.session_state[emp]["FULL"]["bytes"])
        vend_r = load_any_table_from_bytes(st.session_state[emp]["VENDAS"]["name"], st.session_state[emp]["VENDAS"]["bytes"])
        fisi_r = load_any_table_from_bytes(st.session_state[emp]["ESTOQUE"]["name"], st.session_state[emp]["ESTOQUE"]["bytes"])
        
        return mapear_colunas(full_r, "FULL"), mapear_colunas(vend_r, "VENDAS"), mapear_colunas(fisi_r, "FISICO")

    full_A, vend_A, fisi_A = read_and_map(emp_a)
    full_J, vend_J, fisi_J = read_and_map(emp_j)
    
    # 1. Agregação FULL
    full_conjunta = pd.merge(full_A, full_J, on="SKU", how="outer", suffixes=("_A", "_J")).fillna(0)
    full_conjunta["Vendas_Qtd_60d"] = full_conjunta["Vendas_Qtd_60d_A"] + full_conjunta["Vendas_Qtd_60d_J"]
    full_conjunta["Estoque_Full"] = full_conjunta["Estoque_Full_A"] + full_conjunta["Estoque_Full_J"]
    # INSTRUÇÃO: Em_Transito é mantido para o CÁLCULO, mas removido na saída final
    full_conjunta["Em_Transito"] = full_conjunta["Em_Transito_A"] + full_conjunta["Em_Transito_J"]
    full_df_final = full_conjunta[["SKU", "Vendas_Qtd_60d", "Estoque_Full", "Em_Transito"]].copy()

    # 2. Agregação VENDAS (Shopee/MT)
    vend_conjunta = pd.merge(vend_A, vend_J, on="SKU", how="outer", suffixes=("_A", "_J")).fillna(0)
    vend_conjunta["Quantidade"] = vend_conjunta["Quantidade_A"] + vend_conjunta["Quantidade_J"]
    vend_df_final = vend_conjunta[["SKU", "Quantidade"]].copy()
    
    # 3. Agregação FÍSICO
    fisi_conjunta = pd.merge(fisi_A, fisi_J, on="SKU", how="outer", suffixes=("_A", "_J")).fillna(0)
    fisi_conjunta["Estoque_Fisico"] = fisi_conjunta["Estoque_Fisico_A"] + fisi_conjunta["Estoque_Fisico_J"]
    
    # Preço: Usa o preço mais alto/não zero.
    fisi_conjunta["Preco"] = np.where(fisi_conjunta["Preco_A"] > fisi_conjunta["Preco_J"], fisi_conjunta["Preco_A"], fisi_conjunta["Preco_J"])
    fisi_conjunta["Preco"] = np.where(fisi_conjunta["Preco"] == 0, np.maximum(fisi_conjunta["Preco_A"], fisi_conjunta["Preco_J"]), fisi_conjunta["Preco"])
    fisi_df_final = fisi_conjunta[["SKU", "Estoque_Fisico", "Preco"]].copy()

    return full_df_final, fisi_df_final, vend_df_final

# ===================== EXPORT XLSX (sem alteração) =====================
def sha256_of_csv(df: pd.DataFrame) -> str:
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    return hashlib.sha256(csv_bytes).hexdigest()

def exportar_xlsx(df_final: pd.DataFrame, h: int, params: dict, pendencias: list | None = None) -> bytes:
    int_cols = [c for c in ["Estoque_Fisico","Compra_Sugerida","ML_60d","Shopee_60d","TOTAL_60d"] if c in df_final.columns]
    for c in int_cols:
        bad = df_final.index[(df_final[c] < 0) | (df_final[c].astype(float) % 1 != 0)]
        if len(bad) > 0:
            linha = int(bad[0]) + 2
            sku = df_final.loc[bad[0], "SKU"] if "SKU" in df_final.columns else "?"
            raise RuntimeError(f"Auditoria: coluna '{c}' precisa ser inteiro ≥ 0. Ex.: linha {linha} (SKU={sku}).")

    calc = (df_final["Compra_Sugerida"] * df_final["Preco"]).round(2).values
    if not np.allclose(df_final["Valor_Compra_R$"].values, calc):
        bad = np.where(~np.isclose(df_final["Valor_Compra_R$"].values, calc))[0]
        linha = int(bad[0]) + 2 if len(bad) else "?"
        sku = df_final.iloc[bad[0]]["SKU"] if len(bad) and "SKU" in df_final.columns else "?"
        raise RuntimeError(f"Auditoria: 'Valor_Compra_R$' ≠ 'Compra_Sugerida × Preco'. Ex.: linha {linha} (SKU={sku}).")

    hash_str = sha256_of_csv(df_final)
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as w:
        lista = df_final[df_final["Compra_Sugerida"] > 0].copy()
        lista.to_excel(w, sheet_name="Lista_Final", index=False)
        ws = w.sheets["Lista_Final"]
        for i, col in enumerate(lista.columns):
            width = max(12, int(lista[col].astype(str).map(len).max()) + 2)
            ws.set_column(i, i, min(width, 40))
        ws.freeze_panes(1, 0); ws.autofilter(0, 0, len(lista), len(lista.columns)-1)

        if pendencias:
            pd.DataFrame(pendencias).to_excel(w, sheet_name="Pendencias", index=False)

        ctrl = pd.DataFrame([{
            "data_hora": dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "h": h,
            "linhas_Lista_Final": int((df_final["Compra_Sugerida"] > 0).sum()),
            "soma_Compra_Sugerida": int(df_final["Compra_Sugerida"].sum()),
            "soma_Valor_Compra_R$": float(df_final["Valor_Compra_R$"].sum()),
            "hash_sha256": hash_str,
        } | params])
        ctrl.to_excel(w, sheet_name="Controle", index=False)
    output.seek(0)
    return output.read()

# ===================== UI: SIDEBAR (PADRÃO) =====================
with st.sidebar:
    st.subheader("Parâmetros")
    h  = st.selectbox("Horizonte (dias)", [30, 60, 90], index=1)
    g  = st.number_input("Crescimento % ao mês", value=0.0, step=1.0)
    LT = st.number_input("Lead time (dias)", value=0, step=1, min_value=0)

    st.markdown("---")
    st.subheader("Padrão (KITS/CAT) — Google Sheets")
    st.caption("Carrega **somente** quando você clicar. ID fixo da planilha foi deixado no código.")
    colA, colB = st.columns([1, 1])
    with colA:
        if st.button("Carregar padrão agora", use_container_width=True):
            # Limpa o cache para forçar um novo download e ESTABILIZAR o app
            baixar_xlsx_do_sheets.clear() 
            try:
                cat = carregar_padrao_do_xlsx(DEFAULT_SHEET_ID)
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
            cat = carregar_padrao_do_link(st.session_state.alt_sheet_link.strip())
            st.session_state.catalogo_df = cat.catalogo_simples.rename(columns={"sku":"component_sku"})
            st.session_state.kits_df = cat.kits_reais
            st.session_state.loaded_at = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            st.success("Padrão carregado (link alternativo).")
        except Exception as e:
            st.session_state.catalogo_df = None; st.session_state.kits_df = None; st.session_state.loaded_at = None
            st.error(str(e))

# ===================== TÍTULO =====================
st.title("Reposição Logística — Alivvia")
if st.session_state.catalogo_df is None or st.session_state.kits_df is None:
    st.warning("► Carregue o **Padrão (KITS/CAT)** no sidebar antes de usar as abas.")

# ===================== ABAS =====================

tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📂 Dados das Empresas", 
    "🧮 Compra Automática", 
    "📦 Alocação de Compra", 
    "🛒 Ordem de Compra (OC)", 
    "✨ Gerenciador de OCs"
])

# ===================== FUNÇÃO AUXILIAR PARA ALOCAÇÃO DE COMPRA =====================
def _calcular_vendas_componente(full_df, shp_df, cat: Catalogo) -> pd.DataFrame:
    kits = construir_kits_efetivo(cat)
    a = explodir_por_kits(full_df[["SKU","Vendas_Qtd_60d"]].rename(columns={"SKU":"kit_sku","Vendas_Qtd_60d":"Qtd"}), kits,"kit_sku","Qtd")
    a = a.rename(columns={"Quantidade":"ML_60d"})
    b = explodir_por_kits(shp_df[["SKU","Quantidade"]].rename(columns={"SKU":"kit_sku","Quantidade":"Qtd"}), kits,"kit_sku","Qtd")
    b = b.rename(columns={"Quantidade":"Shopee_60d"})
    out = pd.merge(a, b, on="SKU", how="outer").fillna(0)
    out["Demanda_60d"] = out["ML_60d"].astype(int) + out["Shopee_60d"].astype(int)
    return out[["SKU","Demanda_60d", "ML_60d", "Shopee_60d"]]

# ---------- TAB 1: UPLOADS (sem alteração) ----------
with tab1:
    st.subheader("Uploads fixos por empresa (mantidos até você limpar)")
    st.caption("Salvamos FULL, Shopee/MT e Estoque por empresa na sessão. Eles permanecem após F5.")

    def bloco_empresa(emp: str):
        st.markdown(f"### {emp}")
        c1, c2 = st.columns(2)
        # FULL
        with c1:
            st.markdown(f"**FULL — {emp}**")
            up_full = st.file_uploader("CSV/XLSX/XLS", type=["csv","xlsx","xls"], key=f"up_full_{emp}")
            if up_full is not None:
                st.session_state[emp]["FULL"]["name"]  = up_full.name
                st.session_state[emp]["FULL"]["bytes"] = up_full.read()
                st.success(f"FULL carregado: {up_full.name}")
            if st.session_state[emp]["FULL"]["name"]:
                st.caption(f"FULL salvo: **{st.session_state[emp]['FULL']['name']}**")
        # Shopee/MT
        with c2:
            st.markdown(f"**Shopee/MT — {emp}**")
            up_v = st.file_uploader("CSV/XLSX/XLS", type=["csv","xlsx","xls"], key=f"up_v_{emp}")
            if up_v is not None:
                st.session_state[emp]["VENDAS"]["name"]  = up_v.name
                st.session_state[emp]["VENDAS"]["bytes"] = up_v.read()
                st.success(f"Vendas carregado: {up_v.name}")
            if st.session_state[emp]["VENDAS"]["name"]:
                st.caption(f"Vendas salvo: **{st.session_state[emp]['VENDAS']['name']}**")

        # Estoque Físico (opcional para compra)
        st.markdown("**Estoque Físico — (necessário para Compra Automática)**")
        up_e = st.file_uploader("CSV/XLSX/XLS", type=["csv","xlsx","xls"], key=f"up_e_{emp}")
        if up_e is not None:
            st.session_state[emp]["ESTOQUE"]["name"]  = up_e.name
            st.session_state[emp]["ESTOQUE"]["bytes"] = up_e.read()
            st.success(f"Estoque carregado: {up_e.name}")
        if st.session_state[emp]["ESTOQUE"]["name"]:
            st.caption(f"Estoque salvo: **{st.session_state[emp]['ESTOQUE']['name']}**")

        c3, c4 = st.columns([1,1])
        with c3:
            if st.button(f"Salvar {emp}", use_container_width=True, key=f"save_{emp}"):
                st.success(f"Status {emp}: FULL [{'OK' if st.session_state[emp]['FULL']['name'] else '–'}] • "
                           f"Shopee [{'OK' if st.session_state[emp]['VENDAS']['name'] else '–'}] • "
                           f"Estoque [{'OK' if st.session_state[emp]['ESTOQUE']['name'] else '–'}]")
        with c4:
            if st.button(f"Limpar {emp}", use_container_width=True, key=f"clr_{emp}"):
                st.session_state[emp] = {"FULL":{"name":None,"bytes":None},
                                         "VENDAS":{"name":None,"bytes":None},
                                         "ESTOQUE":{"name":None,"bytes":None}}
                st.info(f"{emp} limpo.")

        st.divider()

    bloco_empresa("ALIVVIA")
    bloco_empresa("JCA")

# ---------- TAB 2: COMPRA AUTOMÁTICA (PATCH V4.23.0) ----------
with tab2:
    st.subheader("Gerar Compra (por empresa ou conjunta) — lógica original")

    if st.session_state.catalogo_df is None or st.session_state.kits_df is None:
        st.info("Carregue o **Padrão (KITS/CAT)** no sidebar antes de usar as abas.")
    else:
        # 1. Seleção de Empresa/Conjunta
        empresa_selecionada = st.radio("Empresa ativa", ["ALIVVIA", "JCA", "CONJUNTA"], horizontal=True, key="empresa_ca")
        
        nome_estado = empresa_selecionada
        
        # Lógica de validação visual para CONJUNTA
        if nome_estado == "CONJUNTA":
            missing_conjunta = []
            for emp in ["ALIVVIA", "JCA"]:
                for k, rot in [("FULL", "FULL"), ("VENDAS", "Shopee/MT"), ("ESTOQUE", "Estoque")]:
                    if not (st.session_state[emp][k]["name"] and st.session_state[emp][k]["bytes"]):
                        missing_conjunta.append(f"{emp} {rot}")
            if missing_conjunta:
                st.warning("Para a Compra Conjunta, todos os 6 arquivos (FULL/Shopee/Estoque para ALIVVIA e JCA) devem ser carregados.")
                st.info(f"ALIVVIA: FULL ({st.session_state['ALIVVIA']['FULL']['name'] or '—'}), Shopee ({st.session_state['ALIVVIA']['VENDAS']['name'] or '—'}), Estoque ({st.session_state['ALIVVIA']['ESTOQUE']['name'] or '—'})")
                st.info(f"JCA: FULL ({st.session_state['JCA']['FULL']['name'] or '—'}), Shopee ({st.session_state['JCA']['VENDAS']['name'] or '—'}), Estoque ({st.session_state['JCA']['ESTOQUE']['name'] or '—'})")
            else:
                st.info("Arquivos agregados prontos para o cálculo Conjunto.")
        else:
            dados_display = st.session_state[nome_estado]
            col = st.columns(3)
            col[0].info(f"FULL: {dados_display['FULL']['name'] or '—'}")
            col[1].info(f"Shopee/MT: {dados_display['VENDAS']['name'] or '—'}")
            col[2].info(f"Estoque: {dados_display['ESTOQUE']['name'] or '—'}")

        # 2. Lógica de Disparo (ou manutenção do estado)
        
        # O botão força o recálculo, garantindo que novos uploads sejam processados.
        if st.button(f"Gerar Compra — {nome_estado}", type="primary"):
            st.session_state.compra_autom_data["force_recalc"] = True
        
        # Dispara o cálculo se for forçado OU se o estado não existir
        if nome_estado not in st.session_state.compra_autom_data or st.session_state.compra_autom_data.get("force_recalc", False):
            
            st.session_state.compra_autom_data["force_recalc"] = False
            
            # BLOCO DE CÁLCULO
            try:
                # 1. VALIDAÇÃO E LEITURA DE DADOS
                if nome_estado == "CONJUNTA":
                    
                    missing_conjunta_calc = []
                    for emp in ["ALIVVIA", "JCA"]:
                        for k, rot in [("FULL", "FULL"), ("VENDAS", "Shopee/MT"), ("ESTOQUE", "Estoque")]:
                            if not (st.session_state[emp][k]["name"] and st.session_state[emp][k]["bytes"]):
                                missing_conjunta_calc.append(f"{emp} {rot}")
                    if missing_conjunta_calc:
                        raise RuntimeError("Arquivos necessários para Compra Conjunta estão ausentes (recarregue todos na aba 'Dados das Empresas').")

                    full_df, fisico_df, vendas_df = _aggregate_data_for_conjunta()
                    nome_empresa_calc = "CONJUNTA"
                    
                else: # Individual (ALIVVIA ou JCA)
                    dados = st.session_state[nome_estado]
                    for k, rot in [("FULL","FULL"),("VENDAS","Shopee/MT"),("ESTOQUE","Estoque")]:
                        if not (dados[k]["name"] and dados[k]["bytes"]):
                            raise RuntimeError(f"Arquivo '{rot}' não foi salvo para {nome_estado}. Vá em **Dados das Empresas** e salve.")
                            
                    full_raw   = load_any_table_from_bytes(dados["FULL"]["name"], dados["FULL"]["bytes"])
                    vendas_raw = load_any_table_from_bytes(dados["VENDAS"]["name"], dados["VENDAS"]["bytes"])
                    fisico_raw = load_any_table_from_bytes(dados["ESTOQUE"]["name"], dados["ESTOQUE"]["bytes"])
                    
                    t_full = mapear_tipo(full_raw); t_v = mapear_tipo(vendas_raw); t_f = mapear_tipo(fisico_raw)
                    if t_full != "FULL" or t_v != "VENDAS" or t_f != "FISICO":
                         raise RuntimeError("Um ou mais arquivos (FULL/VENDAS/FISICO) estão com formato incorreto.")

                    full_df   = mapear_colunas(full_raw, t_full)
                    vendas_df = mapear_colunas(vendas_raw, t_v)
                    fisico_df = mapear_colunas(fisico_raw, t_f)
                    nome_empresa_calc = nome_estado

                # 2. CÁLCULO PRINCIPAL
                cat = Catalogo(
                    catalogo_simples=st.session_state.catalogo_df.rename(columns={"sku":"component_sku"}),
                    kits_reais=st.session_state.kits_df
                )
                df_final, painel = calcular(full_df, fisico_df, vendas_df, cat, h=h, g=g, LT=LT)
                
                df_final["Selecionar"] = False # Adiciona coluna de seleção
                
                # SALVA NO ESTADO (CACHING)
                st.session_state.compra_autom_data[nome_estado] = {
                    "df": df_final,
                    "painel": painel,
                    "empresa": nome_empresa_calc
                }
                
                st.success("Cálculo concluído. Selecione itens abaixo para Ordem de Compra.")

            except Exception as e:
                st.session_state.compra_autom_data[nome_estado] = {"error": str(e)}
                st.error(str(e))
        
        # 3. RENDERIZAÇÃO DE RESULTADOS (USANDO O ESTADO SALVO)
        if nome_estado in st.session_state.compra_autom_data and "df" in st.session_state.compra_autom_data[nome_estado]:
            
            data_fixa = st.session_state.compra_autom_data[nome_estado]
            df_final = data_fixa["df"].copy() # Usa uma cópia para filtrar
            painel = data_fixa["painel"]
            nome_empresa_calc = data_fixa["empresa"]
            
            # Mensagem para Compra Conjunta
            if nome_empresa_calc == "CONJUNTA":
                st.warning("⚠️ Compra Conjunta gerada! Use a aba **'📦 Alocação de Compra'** para fracionar o lote sugerido.")
            
            # Renderização do Painel
            cA, cB, cC, cD = st.columns(4)
            cA.metric("Full (un)",  f"{painel['full_unid']:,}".replace(",", "."))
            cB.metric("Full (R$)",  f"R$ {painel['full_valor']:,.2f}")
            cC.metric("Físico (un)",f"{painel['fisico_unid']:,}".replace(",", "."))
            cD.metric("Físico (R$)",f"R$ {painel['fisico_valor']:,.2f}")

            # FILTROS DINÂMICOS
            c_filtros = st.columns(2)
            
            # Filtro Fornecedor
            fornecedores = sorted(df_final["fornecedor"].unique().tolist())
            filtro_forn = c_filtros[0].multiselect("Filtrar Fornecedor", fornecedores)
            
            # Filtro SKU (Busca Parcial - Case Insensitive)
            filtro_sku_text = c_filtros[1].text_input("Buscar SKU/Parte do SKU", key=f"filtro_sku_{nome_estado}").strip()
            
            # Aplicação dos Filtros
            df_filtrado = df_final.copy()

            if filtro_forn:
                df_filtrado = df_filtrado[df_filtrado["fornecedor"].isin(filtro_forn)]

            if filtro_sku_text:
                # Lógica de correspondência parcial (Case-Insensitive)
                df_filtrado = df_filtrado[df_filtrado["SKU"].str.contains(filtro_sku_text, case=False)]

            # 5. TABELA COM CHECKBOX (Ticar)
            df_para_editor = df_filtrado[df_filtrado["Compra_Sugerida"] > 0].reset_index(drop=True)
            
            # Reconstroi a chave de edição para manter a seleção do usuário
            editor_key = f"data_editor_{nome_estado}_{len(df_filtrado)}"
            
            st.data_editor(df_para_editor, key=editor_key, use_container_width=True, height=500,
                column_config={
                    "Selecionar": st.column_config.CheckboxColumn("Selecionar", default=False)
                })
            
            # 6. LÓGICA DO BOTÃO ENVIAR PARA OC (Garantido!)
            df_edited = st.session_state[editor_key]
            df_selecionados = df_edited[df_edited["Selecionar"] == True].copy()
            
            if df_selecionados.empty:
                st.button(f"Enviar 0 itens selecionados para a Cesta de OC", disabled=True)
            else:
                if st.button(f"Enviar {len(df_selecionados)} itens selecionados para a Cesta de OC", type="secondary"):
                    df_selecionados["Empresa"] = nome_empresa_calc
                    df_selecionados = df_selecionados[df_selecionados["Compra_Sugerida"] > 0]
                    
                    # Agrega/Substitui na cesta global
                    if st.session_state.oc_cesta.empty:
                        st.session_state.oc_cesta = df_selecionados
                    else:
                        # Remove itens da mesma empresa/tipo e adiciona os novos
                        cesta_atual = st.session_state.oc_cesta[st.session_state.oc_cesta["Empresa"] != nome_empresa_calc].copy()
                        st.session_state.oc_cesta = pd.concat([cesta_atual, df_selecionados], ignore_index=True)

                    st.success(f"Itens de {nome_empresa_calc} enviados para a Cesta de OC. Total na Cesta: {len(st.session_state.oc_cesta)} itens.")
                    st.dataframe(st.session_state.oc_cesta, use_container_width=True)

            if st.checkbox("Gerar XLSX (Lista_Final + Controle)", key="chk_xlsx"):
                xlsx = exportar_xlsx(df_final, h=h, params={"g":g,"LT":LT,"empresa":nome_empresa_calc})
                st.download_button(
                    "Baixar XLSX", data=xlsx,
                    file_name=f"Compra_Sugerida_{nome_empresa_calc}_{h}d.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

# ---------- TAB 3: ALOCAÇÃO DE COMPRA (sem alteração) ----------
with tab3:
    st.subheader("📦 Alocação de Compra — Fracionar Lote por Proporção de Vendas")

    if st.session_state.catalogo_df is None or st.session_state.kits_df is None:
        st.info("Carregue o **Padrão (KITS/CAT)** no sidebar antes de usar as abas.")
    else:
        CATALOGO = st.session_state.catalogo_df
        
        missing_data = False
        for emp in ["ALIVVIA", "JCA"]:
            if not (st.session_state[emp]["FULL"]["name"] and st.session_state[emp]["VENDAS"]["name"]):
                 missing_data = True
                 break
        
        if missing_data:
            st.warning("É necessário carregar os arquivos **FULL** e **Shopee/MT (Vendas)** para AMBAS as empresas na aba **Dados das Empresas**.")
        
        else:
            try:
                # 1. Leitura de Dados
                def read_pair_alloc(emp: str) -> Tuple[pd.DataFrame,pd.DataFrame]:
                    fa = load_any_table_from_bytes(st.session_state[emp]["FULL"]["name"],   st.session_state[emp]["FULL"]["bytes"])
                    sa = load_any_table_from_bytes(st.session_state[emp]["VENDAS"]["name"], st.session_state[emp]["VENDAS"]["bytes"])
                    
                    tfa = mapear_tipo(fa); tsa = mapear_tipo(sa)
                    if tfa != "FULL":   raise RuntimeError(f"FULL inválido ({emp}): precisa de SKU e Vendas_60d/Estoque_full.")
                    if tsa != "VENDAS": raise RuntimeError(f"Vendas inválido ({emp}): não achei coluna de quantidade.")
                    
                    return mapear_colunas(fa, tfa), mapear_colunas(sa, tsa)

                full_A, shp_A = read_pair_alloc("ALIVVIA")
                full_J, shp_J = read_pair_alloc("JCA")
                
                cat_obj = Catalogo(
                    catalogo_simples=CATALOGO.rename(columns={"sku":"component_sku"}),
                    kits_reais=st.session_state.kits_df
                )

                # 2. Cálculo das Vendas Agregadas por Componente
                demA = _calcular_vendas_componente(full_A, shp_A, cat_obj).rename(columns={"Demanda_60d":"Demanda_A"})
                demJ = _calcular_vendas_componente(full_J, shp_J, cat_obj).rename(columns={"Demanda_60d":"Demanda_J"})
                
                demanda_comp = pd.merge(demA[["SKU", "Demanda_A"]], demJ[["SKU", "Demanda_J"]], on="SKU", how="outer").fillna(0)
                demanda_comp["TOTAL_60d"] = demanda_comp["Demanda_A"] + demanda_comp["Demanda_J"]
                
                # 3. Seleção do SKU e Quantidade
                sku_opcoes = demanda_comp["SKU"].unique().tolist()
                
                col_sel, col_qtd = st.columns([2, 1])
                with col_sel:
                    sku_escolhido = st.selectbox("SKU do componente para alocar", sku_opcoes, key="alloc_sku")
                with col_qtd:
                    qtd_lote = st.number_input("Quantidade total do lote (ex.: 500 un)", min_value=1, value=1000, step=50, key="alloc_qtd")

                if st.button("Calcular Alocação Proporcional", type="primary"):
                    
                    sku_norm = norm_sku(sku_escolhido)
                    
                    # 4. Cálculo da Proporção
                    dados_sku = demanda_comp[demanda_comp["SKU"] == sku_norm]
                    
                    if dados_sku.empty:
                         raise ValueError(f"SKU {sku_norm} não encontrado ou sem demanda nas empresas.")
                         
                    dA = dados_sku["Demanda_A"].iloc[0]
                    dJ = dados_sku["Demanda_J"].iloc[0]
                    total = dA + dJ
                    
                    if total == 0:
                        st.warning("SKU encontrado, mas sem vendas nos últimos 60 dias. Usando alocação 50/50.")
                        propA = propJ = 0.5
                    else:
                        propA = dA / total
                        propJ = dJ / total
                    
                    # 5. Cálculo da Alocação
                    alocA = int(round(qtd_lote * propA))
                    alocJ = int(qtd_lote - alocA)

                    # 6. Output
                    res = pd.DataFrame([
                        {"Empresa":"ALIVVIA", "SKU":sku_norm, "Demanda_60d":dA, "Proporção":round(propA,4), "Alocação_Sugerida":alocA},
                        {"Empresa":"JCA",     "SKU":sku_norm, "Demanda_60d":dJ, "Proporção":round(propJ,4), "Alocação_Sugerida":alocJ},
                    ])
                    
                    st.success(f"Lote de {qtd_lote} unidades de **{sku_norm}** alocado com sucesso.")
                    st.dataframe(res, use_container_width=True)
                    st.markdown(f"**Resultado:** ALIVVIA recebe **{alocA}** un. ({round(propA*100, 1)}%) | JCA recebe **{alocJ}** un. ({round(propJ*100, 1)}%)")
                    st.download_button("Baixar alocação (.csv)", data=res.to_csv(index=False).encode("utf-8"),
                                       file_name=f"Alocacao_{sku_norm}_{qtd_lote}.csv", mime="text/csv")
            
            except Exception as e:
                st.error(f"Erro ao calcular Alocação de Compra: {e}")

# ---------- TAB 4: ORDEM DE COMPRA (OC) - CESTA DE ITENS (sem alteração) ----------
with tab4:
    if 'ordem_compra' in globals():
        st.subheader("🛒 Ordem de Compra (OC) - Cesta de Itens")
        
        cesta = st.session_state.oc_cesta
        if cesta.empty:
            st.info("A cesta de Ordem de Compra está vazia. Adicione itens da aba 'Compra Automática'.")
        else:
            st.success(f"Itens prontos para OC: {len(cesta)} itens de {len(cesta['fornecedor'].unique())} fornecedores.")
            st.dataframe(cesta, use_container_width=True)
            
            if st.button("Gerar e Finalizar Ordem de Compra (Módulo OC)", type="primary"):
                st.warning("Esta função requer a implementação do módulo `ordem_compra.py`.")
            
            if st.button("Limpar Cesta de OC", type="secondary"):
                st.session_state.oc_cesta = pd.DataFrame()
                st.rerun()

    else:
        st.error("ERRO: O módulo 'ordem_compra.py' não foi encontrado. As funcionalidades de OC não estão disponíveis.")

# ---------- TAB 5: GERENCIADOR DE OCS - CONTROLE DE RECEBIMENTO (sem alteração) ----------
with tab5:
    if 'gerenciador_oc' in globals():
        st.subheader("✨ Gerenciador de OCs - Controle de Recebimento")
        st.info("O Gerenciador de OCs está pronto para ser chamado a partir do módulo `gerenciador_oc.py`.")
        st.error("⚠️ ERRO CRÍTICO: Não foi possível autenticar com o Google Sheets. Configure 'credentials.json'.")
        st.warning("Isso é esperado no Streamlit Cloud, pois o arquivo 'credentials.json' de autenticação não está presente.")
    else:
        st.error("ERRO: O módulo 'gerenciador_oc.py' não foi encontrado. As funcionalidades de Gerenciamento de OC não estão disponíveis.")

st.caption("© Alivvia — simples, robusto e auditável. (V4.23.0)")