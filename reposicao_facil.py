# reposicao_facil.py
# Reposição Logística — Alivvia (Streamlit)
# ARQUITETURA CONSOLIDADA V2.0 (por Gemini)

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

# ===================== CONFIG BÁSICA =====================
st.set_page_config(page_title="Reposição Logística — Alivvia", layout="wide")

DEFAULT_SHEET_LINK = (
    "https://docs.google.com/spreadsheets/d/1cTLARjq-B5g50dL6tcntg7lb_Iu0ta43/"
    "edit?usp=sharing&ouid=109458533144345974874&rtpof=true&sd=true"
)
DEFAULT_SHEET_ID = "1cTLARjq-B5g50dL6tcntg7lb_Iu0ta43"  # fixo

# ===================== ESTADO =====================
def _ensure_state():
    st.session_state.setdefault("catalogo_df", None)
    st.session_state.setdefault("kits_df", None)
    st.session_state.setdefault("loaded_at", None)
    st.session_state.setdefault("alt_sheet_link", DEFAULT_SHEET_LINK)

    # NOVO: Persistência dos resultados de cálculo
    st.session_state.setdefault("resultado_ALIVVIA", None)
    st.session_state.setdefault("resultado_JCA", None)
    
    # NOVO: Carrinho de compras (Lista de DataFrames, pois são de empresas diferentes)
    st.session_state.setdefault("carrinho_compras", [])

    # uploads por empresa
    for emp in ["ALIVVIA", "JCA"]:
        st.session_state.setdefault(emp, {})
        st.session_state[emp].setdefault("FULL",   {"name": None, "bytes": None})
        st.session_state[emp].setdefault("VENDAS", {"name": None, "bytes": None})
        st.session_state[emp].setdefault("ESTOQUE",{"name": None, "bytes": None})

_ensure_state()

# ===================== HTTP / GOOGLE SHEETS =====================
# Funções inalteradas: _requests_session, gs_export_xlsx_url, extract_sheet_id_from_url, baixar_xlsx_por_link_google, baixar_xlsx_do_sheets
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

def baixar_xlsx_do_sheets(sheet_id: str) -> bytes:
    s = _requests_session()
    url = gs_export_xlsx_url(sheet_id)
    try:
        r = s.get(url, timeout=30)
        r.raise_for_status()
    except requests.HTTPError as e:
        sc = getattr(e.response, "status_code", "?")
        raise RuntimeError(
            f"Falha ao baixar XLSX (HTTP {sc}). Verifique: compartilhamento 'Qualquer pessoa com link – Leitor'.\nURL: {url}"
        )
    return r.content

# ===================== UTILS DE DADOS =====================
# Funções inalteradas: norm_header, normalize_cols, br_to_float, norm_sku, exige_colunas
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
# Funções inalteradas: load_any_table, load_any_table_from_bytes
def load_any_table(uploaded_file) -> Optional[pd.DataFrame]:
    if uploaded_file is None:
        return None
    name = uploaded_file.name.lower()
    try:
        if name.endswith(".csv"):
            uploaded_file.seek(0)
            df = pd.read_csv(uploaded_file, dtype=str, keep_default_na=False, sep=None, engine="python")
        else:
            uploaded_file.seek(0)
            df = pd.read_excel(uploaded_file, dtype=str, keep_default_na=False)
    except Exception as e:
        raise RuntimeError(f"Não consegui ler o arquivo '{uploaded_file.name}': {e}")

    df.columns = [norm_header(c) for c in df.columns]

    # fallback header=2 (FULL Magiic)
    tem_col_sku = any(c in df.columns for c in ["sku","codigo","codigo_sku"]) or any("sku" in c for c in df.columns)
    if (not tem_col_sku) and (len(df) > 0):
        try:
            uploaded_file.seek(0)
            if name.endswith(".csv"):
                df = pd.read_csv(uploaded_file, dtype=str, keep_default_na=False, header=2)
            else:
                df = pd.read_excel(uploaded_file, dtype=str, keep_default_na=False, header=2)
            df.columns = [norm_header(c) for c in df.columns]
        except Exception:
            pass

    # limpeza
    cols = set(df.columns)
    sku_col = next((c for c in ["sku","codigo","codigo_sku"] if c in cols), None)
    if sku_col:
        df[sku_col] = df[sku_col].map(norm_sku)
        df = df[df[sku_col] != ""]
    for c in list(df.columns):
        df = df[~df[c].astype(str).str.contains(r"^TOTALS?$|^TOTAIS?$", case=False, na=False)]
    return df.reset_index(drop=True)

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
# Classes e funções inalteradas: Catalogo, _carregar_padrao_de_content, carregar_padrao_do_xlsx, carregar_padrao_do_link, construir_kits_efetivo
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

    # KITS
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

    # CATALOGO
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

# ===================== MAPEAMENTO FULL/FISICO/VENDAS =====================
# Funções inalteradas: mapear_tipo, mapear_colunas
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

# ===================== KITS (EXPLOSÃO) =====================
# Função inalterada: explodir_por_kits
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

# ===================== COMPRA AUTOMÁTICA (LÓGICA ORIGINAL) =====================
# ATENÇÃO: Alteração na seleção final de colunas para atender ao novo requisito de layout
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
    
    # NOVO: Vendas Total 60d (soma simples)
    demanda["Vendas_Total_60d"] = demanda["ML_60d"] + demanda["Shopee_60d"] 

    fis = fisico_df.copy()
    fis["SKU"] = fis["SKU"].map(norm_sku)
    fis["Estoque_Fisico"] = fis["Estoque_Fisico"].fillna(0).astype(int)
    fis["Preco"] = fis["Preco"].fillna(0.0)

    base = demanda.merge(fis, on="SKU", how="left")
    base["Estoque_Fisico"] = base["Estoque_Fisico"].fillna(0).astype(int)
    base["Preco"] = base["Preco"].fillna(0.0)

    # Adicionar Estoque_Full do FULL
    base = base.merge(full[["SKU", "Estoque_Full"]], on="SKU", how="left").fillna({"Estoque_Full": 0})
    base["Estoque_Full"] = base["Estoque_Full"].astype(int)

    fator = (1.0 + g/100.0) ** (h/30.0)
    fk = full.copy()
    fk["vendas_dia"] = fk["Vendas_Qtd_60d"] / 60.0
    fk["alvo"] = np.round(fk["vendas_dia"] * (LT + h) * fator).astype(int)
    fk["oferta"] = (fk["Estoque_Full"] + fk["Em_Transito"]).astype(int)
    fk["envio_desejado"] = (fk["alvo"] - fk["oferta"]).clip(lower=0).astype(int)

    necessidade = explodir_por_kits(
        fk[["SKU","envio_desejado"]].rename(columns={"SKU":"kit_sku","envio_desejado":"Qtd"}),
        kits,"kit_sku","Qtd").rename(columns={"Quantidade":"Necessidade"})

    base = base.merge(necessidade, on="SKU", how="left")
    base["Necessidade"] = base["Necessidade"].fillna(0).astype(int)

    base["Demanda_dia"]  = base["TOTAL_60d"] / 60.0
    base["Reserva_30d"]  = np.round(base["Demanda_dia"] * 30).astype(int)
    base["Folga_Fisico"] = (base["Estoque_Fisico"] - base["Reserva_30d"]).clip(lower=0).astype(int)

    base["Compra_Sugerida"] = (base["Necessidade"] - base["Folga_Fisico"]).clip(lower=0).astype(int)

    mask_nao = base["status_reposicao"].str.lower().str.contains("nao_repor", na=False)
    base.loc[mask_nao, "Compra_Sugerida"] = 0

    base["Valor_Compra_R$"] = (base["Compra_Sugerida"].astype(float) * base["Preco"].astype(float)).round(2)
    
    # ATENÇÃO: Seleção das colunas finais de acordo com a sua solicitação
    df_final = base[[
        "SKU","fornecedor",
        "Vendas_Total_60d", # NOVO
        "Estoque_Full",     # NOVO
        "Estoque_Fisico","Preco","Compra_Sugerida","Valor_Compra_R$",
        "ML_60d","Shopee_60d","TOTAL_60d","Reserva_30d","Folga_Fisico","Necessidade" # Colunas de auditoria
    ]].reset_index(drop=True)

    # Painel (mantido o original para métricas)
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

# ===================== EXPORT CSV / STYLER =====================
def exportar_carrinho_csv(df: pd.DataFrame) -> bytes:
    df["Data_Hora_OC"] = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return df.to_csv(index=False).encode("utf-8")

def style_df_compra(df: pd.DataFrame):
    """Aplica o destaque na coluna Compra_Sugerida."""
    # Definição do formato
    format_mapping = {
        'Estoque_Fisico': '{:,}'.replace(",", "."),
        'Compra_Sugerida': '{:,}'.replace(",", "."),
        'Vendas_Total_60d': '{:,}'.replace(",", "."),
        'Estoque_Full': '{:,}'.replace(",", "."),
        'Preco': 'R$ {:,.2f}'.replace(",", "X").replace(".", ",").replace("X", "."), # R$ 0.000,00
        'Valor_Compra_R$': 'R$ {:,.2f}'.replace(",", "X").replace(".", ",").replace("X", ".")
    }
    
    # Aplica o formato nas colunas existentes
    styler = df.style.format({c: fmt for c, fmt in format_mapping.items() if c in df.columns})
    
    # Aplica cor de fundo se Compra_Sugerida for > 0
    def highlight_compra(s):
        is_compra = s.name == 'Compra_Sugerida'
        if is_compra:
            return ['background-color: #A93226; color: white' if v > 0 else '' for v in s]
        return ['' for _ in s]

    styler = styler.apply(highlight_compra, axis=0, subset=['Compra_Sugerida'])
    
    return styler

# ===================== UI: SIDEBAR (PADRÃO) =====================
with st.sidebar:
    st.subheader("Parâmetros")
    h  = st.selectbox("Horizonte (dias)", [30, 60, 90], index=1, key="param_h")
    g  = st.number_input("Crescimento % ao mês", value=0.0, step=1.0, key="param_g")
    LT = st.number_input("Lead time (dias)", value=0, step=1, min_value=0, key="param_lt")

    st.markdown("---")
    st.subheader("Padrão (KITS/CAT) — Google Sheets")
    st.caption("Carrega **somente** quando você clicar. ID fixo da planilha foi deixado no código.")
    colA, colB = st.columns([1, 1])
    with colA:
        if st.button("Carregar padrão agora", use_container_width=True):
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
            st.session_state.catalogo_df = cat.catalogo_simples.rename(columns={"component_sku":"sku"})
            st.session_state.kits_df = cat.kits_reais
            st.session_state.loaded_at = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            st.success("Padrão carregado (link alternativo).")
        except Exception as e:
            st.session_state.catalogo_df = None; st.session_state.kits_df = None; st.session_state.loaded_at = None
            st.error(str(e))
            
    if st.session_state.loaded_at:
        st.caption(f"Padrão carregado em: {st.session_state.loaded_at}")

# ===================== TÍTULO =====================
st.title("Reposição Logística — Alivvia")
if st.session_state.catalogo_df is None or st.session_state.kits_df is None:
    st.warning("► Carregue o **Padrão (KITS/CAT)** no sidebar antes de usar as abas.")

# ===================== ABAS (NOVA ESTRUTURA) =====================
tab1, tab2, tab3, tab4 = st.tabs([
    "📂 Dados das Empresas", 
    "🔍 Análise de Compra (Consolidado)", 
    "🛒 Pedido de Compra",
    "📦 Alocação de Compra"
])

# ---------- TAB 1: UPLOADS ----------
with tab1:
    st.subheader("Uploads fixos por empresa (mantidos até você limpar)")
    st.caption("Salvamos FULL e Shopee/MT (e opcionalmente Estoque) por empresa na sessão. Clique **Salvar** para fixar.")

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
        st.markdown("**Estoque Físico — opcional (necessário só para Compra Automática)**")
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
                # Limpa também o resultado do cálculo
                st.session_state[f"resultado_{emp}"] = None
                st.info(f"{emp} limpo e resultado de cálculo zerado.")

        st.divider()

    bloco_empresa("ALIVVIA")
    bloco_empresa("JCA")

# ---------- TAB 2: ANÁLISE DE COMPRA (CONSOLIDADO) ----------
with tab2:
    st.subheader("Gerar e Analisar Compra (Consolidado) — Lógica Original")

    if st.session_state.catalogo_df is None or st.session_state.kits_df is None:
        st.info("Carregue o **Padrão (KITS/CAT)** no sidebar.")
    else:
        
        # --- Cálculo/Persistência ---
        def run_calculo(empresa: str):
            dados = st.session_state[empresa]
            try:
                # valida presença
                for k, rot in [("FULL","FULL"),("VENDAS","Shopee/MT"),("ESTOQUE","Estoque")]:
                    if not (dados[k]["name"] and dados[k]["bytes"]):
                        raise RuntimeError(f"Arquivo '{rot}' não foi salvo para {empresa}.")

                # leitura pelos BYTES
                full_raw   = load_any_table_from_bytes(dados["FULL"]["name"], dados["FULL"]["bytes"])
                vendas_raw = load_any_table_from_bytes(dados["VENDAS"]["name"], dados["VENDAS"]["bytes"])
                fisico_raw = load_any_table_from_bytes(dados["ESTOQUE"]["name"], dados["ESTOQUE"]["bytes"])

                # tipagem
                t_full = mapear_tipo(full_raw)
                t_v    = mapear_tipo(vendas_raw)
                t_f    = mapear_tipo(fisico_raw)
                if t_full != "FULL":   raise RuntimeError("FULL inválido.")
                if t_v    != "VENDAS": raise RuntimeError("Vendas inválido.")
                if t_f    != "FISICO": raise RuntimeError("Estoque inválido.")

                full_df   = mapear_colunas(full_raw, t_full)
                vendas_df = mapear_colunas(vendas_raw, t_v)
                fisico_df = mapear_colunas(fisico_raw, t_f)

                cat = Catalogo(
                    catalogo_simples=st.session_state.catalogo_df.rename(columns={"sku":"component_sku"}),
                    kits_reais=st.session_state.kits_df
                )
                df_final, painel = calcular(full_df, fisico_df, vendas_df, cat, h=st.session_state.param_h, g=st.session_state.param_g, LT=st.session_state.param_lt)
                
                # NOVO: Persiste o resultado
                st.session_state[f"resultado_{empresa}"] = df_final
                st.success(f"Cálculo para {empresa} concluído.")
                
            except Exception as e:
                st.error(f"Erro ao calcular {empresa}: {str(e)}")

        colC, colD = st.columns(2)
        with colC:
            if st.button("Gerar Compra — ALIVVIA", type="primary"):
                run_calculo("ALIVVIA")
        with colD:
            if st.button("Gerar Compra — JCA", type="primary"):
                run_calculo("JCA")

        # --- Filtros e Visualização ---
        st.markdown("---")
        st.subheader("Filtros de Análise (Aplicado em Ambas Empresas)")
        
        df_A = st.session_state.resultado_ALIVVIA
        df_J = st.session_state.resultado_JCA
        
        if df_A is None and df_J is None:
            st.info("Gere o cálculo para pelo menos uma empresa acima para visualizar e filtrar.")
        else:
            df_full = pd.concat([df_A, df_J], ignore_index=True) if df_A is not None and df_J is not None else (df_A if df_A is not None else df_J)
            
            # Filtros dinâmicos
            c1, c2 = st.columns(2)
            with c1:
                sku_filter = st.text_input("Filtro por SKU (contém)", key="filt_sku").upper().strip()
            with c2:
                fornecedor_opc = df_full["fornecedor"].unique().tolist() if df_full is not None else []
                fornecedor_opc.insert(0, "TODOS")
                fornecedor_filter = st.selectbox("Filtro por Fornecedor", fornecedor_opc, key="filt_forn")
            
            # Aplica filtros
            def aplicar_filtro(df: pd.DataFrame) -> pd.DataFrame:
                if df is None: return None
                df_filt = df.copy()
                if sku_filter:
                    df_filt = df_filt[df_filt["SKU"].str.contains(sku_filter, na=False)]
                if fornecedor_filter != "TODOS":
                    df_filt = df_filt[df_filt["fornecedor"] == fornecedor_filter]
                return df_filt

            df_A_filt = aplicar_filtro(df_A)
            df_J_filt = aplicar_filtro(df_J)

            # --- Adicionar ao Carrinho ---
            st.markdown("---")
            st.subheader("Seleção de Itens para Compra (Carrinho)")

            if st.button("🛒 Adicionar Itens Selecionados ao Pedido", type="secondary"):
                carrinho = []
                # Processa ALIVVIA
                selec_A = df_A_filt[st.session_state.get('sel_A', [False] * len(df_A_filt))]
                if not selec_A.empty:
                    selec_A = selec_A[selec_A["Compra_Sugerida"] > 0].copy()
                    selec_A["Empresa"] = "ALIVVIA"
                    carrinho.append(selec_A)
                
                # Processa JCA
                selec_J = df_J_filt[st.session_state.get('sel_J', [False] * len(df_J_filt))]
                if not selec_J.empty:
                    selec_J = selec_J[selec_J["Compra_Sugerida"] > 0].copy()
                    selec_J["Empresa"] = "JCA"
                    carrinho.append(selec_J)
                
                if carrinho:
                    # Remove colunas de auditoria para o carrinho
                    cols_carrinho = ["Empresa", "SKU", "fornecedor", "Preco", "Compra_Sugerida", "Valor_Compra_R$"]
                    carrinho_df = pd.concat(carrinho)[cols_carrinho]
                    carrinho_df.columns = ["Empresa", "SKU", "Fornecedor", "Preco_Custo", "Qtd_Sugerida", "Valor_Sugerido_R$"]
                    carrinho_df["Qtd_Ajustada"] = carrinho_df["Qtd_Sugerida"]
                    
                    st.session_state.carrinho_compras = [carrinho_df.reset_index(drop=True)]
                    st.success(f"Adicionado {len(carrinho_df)} itens ao Pedido de Compra.")
                else:
                    st.warning("Nenhum item com Compra Sugerida > 0 foi selecionado.")

            # --- Visualização de Resultados ---
            if df_A_filt is not None and not df_A_filt.empty:
                st.markdown("### ALIVVIA")
                # Cria a coluna de seleção
                df_A_filt["Selecionar"] = st.session_state.get('sel_A', [False] * len(df_A_filt))[:len(df_A_filt)]
                
                col_order = ["Selecionar", "SKU", "fornecedor", "Vendas_Total_60d", "Estoque_Full", "Estoque_Fisico", "Preco", "Compra_Sugerida", "Valor_Compra_R$"]
                
                edited_df_A = st.dataframe(
                    style_df_compra(df_A_filt[col_order]),
                    use_container_width=True,
                    column_order=col_order,
                    column_config={"Selecionar": st.column_config.CheckboxColumn("Comprar", default=False)},
                    key="df_view_A"
                )
                # Atualiza o estado da seleção (após a edição na tabela)
                if edited_df_A:
                    st.session_state.sel_A = edited_df_A["Selecionar"].tolist()

            if df_J_filt is not None and not df_J_filt.empty:
                st.markdown("### JCA")
                # Cria a coluna de seleção
                df_J_filt["Selecionar"] = st.session_state.get('sel_J', [False] * len(df_J_filt))[:len(df_J_filt)]
                
                col_order = ["Selecionar", "SKU", "fornecedor", "Vendas_Total_60d", "Estoque_Full", "Estoque_Fisico", "Preco", "Compra_Sugerida", "Valor_Compra_R$"]
                
                edited_df_J = st.dataframe(
                    style_df_compra(df_J_filt[col_order]),
                    use_container_width=True,
                    column_order=col_order,
                    column_config={"Selecionar": st.column_config.CheckboxColumn("Comprar", default=False)},
                    key="df_view_J"
                )
                # Atualiza o estado da seleção (após a edição na tabela)
                if edited_df_J:
                    st.session_state.sel_J = edited_df_J["Selecionar"].tolist()

# ---------- TAB 3: PEDIDO DE COMPRA ----------
with tab3:
    st.subheader("🛒 Revisão e Finalização do Pedido de Compra")
    
    if not st.session_state.carrinho_compras:
        st.info("O carrinho de compras está vazio. Adicione itens na aba **Análise de Compra (Consolidado)**.")
    else:
        df_carrinho = st.session_state.carrinho_compras[0].copy()
        
        # Auditoria/Detalhes da OC
        st.markdown("---")
        c1, c2 = st.columns(2)
        with c1:
            st.text_input("Fornecedor Principal do Pedido:", key="oc_fornecedor", value=df_carrinho["Fornecedor"].iloc[0] if not df_carrinho.empty else "")
        with c2:
            st.text_input("Número da Ordem de Compra (OC):", key="oc_num")
        st.text_area("Nota/Observação:", key="oc_obs")
        st.markdown("---")

        st.markdown("### Ajuste de Quantidades")
        
        # Configura a coluna Qtd_Ajustada para ser editável (inteiro > 0)
        col_config = {
            "Qtd_Ajustada": st.column_config.NumberColumn(
                "Qtd. Ajustada (Final)",
                help="Quantidade final para compra.",
                min_value=1,
                format="%d",
                default=1
            )
        }
        
        # Exibe o editor de dados
        edited_carrinho = st.data_editor(
            df_carrinho,
            use_container_width=True,
            column_config=col_config,
            disabled=["Empresa", "SKU", "Fornecedor", "Preco_Custo", "Qtd_Sugerida", "Valor_Sugerido_R$"]
        )
        
        # Recalcula o valor total com a quantidade ajustada
        edited_carrinho["Valor_Ajustado_R$"] = (edited_carrinho["Qtd_Ajustada"] * edited_carrinho["Preco_Custo"]).round(2)
        
        # Atualiza o estado para persistir as alterações
        st.session_state.carrinho_compras[0] = edited_carrinho

        # Métricas Finais
        total_unidades = int(edited_carrinho["Qtd_Ajustada"].sum())
        total_valor_oc = float(edited_carrinho["Valor_Ajustado_R$"].sum())
        
        c3, c4 = st.columns(2)
        c3.metric("Total de Itens", f"{len(edited_carrinho)}")
        c4.metric("Valor Total do Pedido", f"R$ {total_valor_oc:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))

        st.markdown("---")
        
        # Botão de Exportação Final (CSV)
        if st.button("📥 Exportar Pedido Final (CSV)", type="primary"):
            df_export = edited_carrinho.copy()
            # Adiciona colunas de auditoria
            df_export["OC_Fornecedor"] = st.session_state.oc_fornecedor
            df_export["OC_Numero"] = st.session_state.oc_num
            df_export["OC_Obs"] = st.session_state.oc_obs
            
            csv = exportar_carrinho_csv(df_export)
            st.download_button(
                "Baixar CSV para Ordem de Compra",
                data=csv,
                file_name=f"OC_{st.session_state.oc_fornecedor.replace(' ', '_')}_{dt.datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv"
            )
            st.success("CSV gerado! Use este arquivo para alimentar o seu relatório de Ordem de Compra no Google Looker Studio.")

# ---------- TAB 4: ALOCAÇÃO DE COMPRA (sem estoque) ----------
with tab4:
    st.subheader("Distribuir quantidade entre empresas — proporcional às vendas (FULL + Shopee)")

    if st.session_state.catalogo_df is None or st.session_state.kits_df is None:
        st.info("Carregue o **Padrão (KITS/CAT)** no sidebar.")
    else:
        CATALOGO = st.session_state.catalogo_df
        sku_opcoes = CATALOGO["sku"].dropna().astype(str).sort_values().unique().tolist()
        sku_escolhido = st.selectbox("SKU do componente para alocar", sku_opcoes, key="alloc_sku")
        qtd_lote = st.number_input("Quantidade total do lote (ex.: 400)", min_value=1, value=1000, step=50, key="alloc_qtd")

        if st.button("Calcular alocação proporcional"):
            try:
                # precisa de FULL e VENDAS salvos para AMBAS as empresas
                missing = []
                for emp in ["ALIVVIA","JCA"]:
                    if not (st.session_state[emp]["FULL"]["name"] and st.session_state[emp]["FULL"]["bytes"]):
                        missing.append(f"{emp} FULL")
                    if not (st.session_state[emp]["VENDAS"]["name"] and st.session_state[emp]["VENDAS"]["bytes"]):
                        missing.append(f"{emp} Shopee/MT")
                if missing:
                    raise RuntimeError("Faltam arquivos salvos: " + ", ".join(missing) + ". Use a aba **Dados das Empresas**.")

                # leitura BYTES
                def read_pair(emp: str) -> Tuple[pd.DataFrame,pd.DataFrame]:
                    fa = load_any_table_from_bytes(st.session_state[emp]["FULL"]["name"],   st.session_state[emp]["FULL"]["bytes"])
                    sa = load_any_table_from_bytes(st.session_state[emp]["VENDAS"]["name"], st.session_state[emp]["VENDAS"]["bytes"])
                    tfa = mapear_tipo(fa); tsa = mapear_tipo(sa)
                    if tfa != "FULL":   raise RuntimeError(f"FULL inválido ({emp}): precisa de SKU e Vendas_60d/Estoque_full.")
                    if tsa != "VENDAS": raise RuntimeError(f"Vendas inválido ({emp}): não achei coluna de quantidade.")
                    return mapear_colunas(fa, tfa), mapear_colunas(sa, tsa)

                full_A, shp_A = read_pair("ALIVVIA")
                full_J, shp_J = read_pair("JCA")

                # explode por kits --> demanda 60d por componente
                cat = Catalogo(
                    catalogo_simples=CATALOGO.rename(columns={"sku":"component_sku"}),
                    kits_reais=st.session_state.kits_df
                )
                kits = construir_kits_efetivo(cat)

                def vendas_componente(full_df, shp_df) -> pd.DataFrame:
                    a = explodir_por_kits(full_df[["SKU","Vendas_Qtd_60d"]].rename(columns={"SKU":"kit_sku","Vendas_Qtd_60d":"Qtd"}), kits,"kit_sku","Qtd")
                    a = a.rename(columns={"Quantidade":"ML_60d"})
                    b = explodir_por_kits(shp_df[["SKU","Quantidade"]].rename(columns={"SKU":"kit_sku","Quantidade":"Qtd"}), kits,"kit_sku","Qtd")
                    b = b.rename(columns={"Quantidade":"Shopee_60d"})
                    out = pd.merge(a, b, on="SKU", how="outer").fillna(0)
                    out["Demanda_60d"] = out["ML_60d"].astype(int) + out["Shopee_60d"].astype(int)
                    return out[["SKU","Demanda_60d"]]

                demA = vendas_componente(full_A, shp_A)
                demJ = vendas_componente(full_J, shp_J)

                dA = int(demA.loc[demA["SKU"]==norm_sku(sku_escolhido), "Demanda_60d"].sum())
                dJ = int(demJ.loc[demJ["SKU"]==norm_sku(sku_escolhido), "Demanda_60d"].sum())

                total = dA + dJ
                if total == 0:
                    st.warning("Sem vendas detectadas; alocação 50/50 por falta de base.")
                    propA = propJ = 0.5
                else:
                    propA = dA / total
                    propJ = dJ / total

                alocA = int(round(qtd_lote * propA))
                alocJ = int(qtd_lote - alocA)

                res = pd.DataFrame([
                    {"Empresa":"ALIVVIA", "SKU":norm_sku(sku_escolhido), "Demanda_60d":dA, "Proporção":round(propA,4), "Alocação_Sugerida":alocA},
                    {"Empresa":"JCA",     "SKU":norm_sku(sku_escolhido), "Demanda_60d":dJ, "Proporção":round(propJ,4), "Alocação_Sugerida":alocJ},
                ])
                st.dataframe(res, use_container_width=True)
                st.success(f"Total alocado: {qtd_lote} un (ALIVVIA {alocA} | JCA {alocJ})")
                st.download_button("Baixar alocação (.csv)", data=res.to_csv(index=False).encode("utf-8"),
                                   file_name=f"Alocacao_{sku_escolhido}_{qtd_lote}.csv", mime="text/csv")
            except Exception as e:
                st.error(str(e))

st.caption("© Alivvia — simples, robusto e auditável. Arquitetura V2.0")