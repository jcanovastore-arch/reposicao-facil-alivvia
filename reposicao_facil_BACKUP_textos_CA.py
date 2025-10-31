# ReposiÃ§Ã£o LogÃ­stica â€” Alivvia (Streamlit)
# v3.3.0 - UI:
# - Limpar arquivos individualmente (FULL/VENDAS/ESTOQUE) na aba Dados
# - Busca de SKU por texto + multiselect (filtros pÃ³s-cÃ¡lculo)
# - Lista combinada (ALIVVIA + JCA) com filtros e download XLSX
# v3.2.3 - UI:
# - Grade enxuta na aba "Compra AutomÃ¡tica" (colunas essenciais)
# - RemoÃ§Ã£o de tabela duplicada
# - Bloco "Consolidado por SKU (ALIVVIA + JCA)"
# v3.2.2 - base:
# - Destaque VERDE para arquivos salvos (badge_ok)
# - PersistÃªncia de uploads em memÃ³ria + disco (.uploads/)
# - Filtros pÃ³s-cÃ¡lculo sem sumir o resultado
# - Aba "AlocaÃ§Ã£o de Compra" restaurada
# - ExibiÃ§Ã£o de versÃ£o na UI
# - Saneamento defensivo de valores negativos

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

import ordem_compra as oc  # módulo de Ordem de Compra

VERSION = "v3.3.0 - 2025-10-21"
st.set_page_config(page_title="Reposicao Logistica - Alivvia", layout="wide")

DEFAULT_SHEET_LINK = (
    "https://docs.google.com/spreadsheets/d/1cTLARjq-B5g50dL6tcntg7lb_Iu0ta43/"
    "edit?usp=sharing&ouid=109458533144345974874&rtpof=true&sd=true"
)
DEFAULT_SHEET_ID = "1cTLARjq-B5g50dL6tcntg7lb_Iu0ta43"

# ================ UI helpers =================
def badge_ok(label: str, filename: str) -> str:
    return f"<span style='background:#198754; color:#fff; padding:6px 10px; border-radius:10px; font-size:12px;'>âœ… {label}: <b>{filename}</b></span>"

# ============ PersistÃªncia em DISCO (.uploads/) ============
BASE_UPLOAD_DIR = ".uploads"

def _disk_dir(emp: str, kind: str) -> str:
    p = os.path.join(BASE_UPLOAD_DIR, emp, kind)
    os.makedirs(p, exist_ok=True)
    return p

def _disk_put(emp: str, kind: str, name: str, blob: bytes):
    p = _disk_dir(emp, kind)
    with open(os.path.join(p, "file.bin"), "wb") as f:
        f.write(blob)
    with open(os.path.join(p, "meta.json"), "w", encoding="utf-8") as f:
        json.dump({"name": name}, f)

def _disk_get(emp: str, kind: str):
    p = _disk_dir(emp, kind)
    meta = os.path.join(p, "meta.json")
    data = os.path.join(p, "file.bin")
    if not (os.path.exists(meta) and os.path.exists(data)):
        return None
    try:
        with open(meta, "r", encoding="utf-8") as f:
            info = json.load(f)
        with open(data, "rb") as f:
            blob = f.read()
        return {"name": info.get("name", "arquivo.bin"), "bytes": blob}
    except Exception:
        return None

def _disk_clear(emp: str):
    p = os.path.join(BASE_UPLOAD_DIR, emp)
    try:
        if os.path.isdir(p):
            for root, _, files in os.walk(p):
                for fn in files:
                    try:
                        os.remove(os.path.join(root, fn))
                    except:
                        pass
    except:
        pass

# >>> NOVO: deletar um arquivo especÃ­fico no disco
def _disk_delete(emp: str, kind: str):
    p = _disk_dir(emp, kind)
    meta = os.path.join(p, "meta.json")
    data = os.path.join(p, "file.bin")
    try:
        if os.path.exists(meta):
            os.remove(meta)
        if os.path.exists(data):
            os.remove(data)
    except:
        pass

# ============ Cofre em memÃ³ria ============
@st.cache_resource(show_spinner=False)
def _file_store():
    return {
        "ALIVVIA": {"FULL": None, "VENDAS": None, "ESTOQUE": None},
        "JCA":     {"FULL": None, "VENDAS": None, "ESTOQUE": None},
    }

def _store_put(emp: str, kind: str, name: str, blob: bytes):
    store = _file_store()
    store[emp][kind] = {"name": name, "bytes": blob}
    _disk_put(emp, kind, name, blob)

def _store_get(emp: str, kind: str):
    store = _file_store()
    it = store[emp][kind]
    if it:
        return it
    it = _disk_get(emp, kind)
    if it:
        store[emp][kind] = it
        return it
    return None

def _store_clear(emp: str):
    store = _file_store()
    store[emp] = {"FULL": None, "VENDAS": None, "ESTOQUE": None}
    _disk_clear(emp)

# >>> NOVO: deletar um arquivo especÃ­fico na memÃ³ria + disco
def _store_delete(emp: str, kind: str):
    store = _file_store()
    store[emp][kind] = {"name": None, "bytes": None}
    _disk_delete(emp, kind)

# =================== Estado ===================
def _ensure_state():
    st.session_state.setdefault("catalogo_df", None)
    st.session_state.setdefault("kits_df", None)
    st.session_state.setdefault("loaded_at", None)
    st.session_state.setdefault("alt_sheet_link", DEFAULT_SHEET_LINK)
    st.session_state.setdefault("resultado_compra", {})

    for emp in ["ALIVVIA", "JCA"]:
        st.session_state.setdefault(emp, {})
        for kind in ["FULL", "VENDAS", "ESTOQUE"]:
            st.session_state[emp].setdefault(kind, {"name": None, "bytes": None})
            if st.session_state[emp][kind]["name"] is None:
                it = _store_get(emp, kind)
                if it:
                    st.session_state[emp][kind] = it

_ensure_state()

# ============ HTTP Google Sheets ============
def _requests_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(total=3, backoff_factor=0.6, status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["GET"])
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.headers.update({"User-Agent": "Mozilla/5.0"})
    return s

def gs_export_xlsx_url(sheet_id: str) -> str:
    return f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=xlsx"

def extract_sheet_id_from_url(url: str) -> Optional[str]:
    if not url:
        return None
    m = re.search(r"/d/([a-zA-Z0-9\-_]+)/", url)
    return m.group(1) if m else None

def baixar_xlsx_por_link_google(url: str) -> bytes:
    s = _requests_session()
    if "export?format=xlsx" in url:
        r = s.get(url, timeout=30)
        r.raise_for_status()
        return r.content
    sid = extract_sheet_id_from_url(url)
    if not sid:
        raise RuntimeError("Link invÃ¡lido do Google Sheets (esperado .../d/<ID>/...).")
    r = s.get(gs_export_xlsx_url(sid), timeout=30)
    r.raise_for_status()
    return r.content

def baixar_xlsx_do_sheets(sheet_id: str) -> bytes:
    s = _requests_session()
    url = gs_export_xlsx_url(sheet_id)
    r = s.get(url, timeout=30)
    r.raise_for_status()
    return r.content

# ============ Utils ============
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
    if pd.isna(x):
        return np.nan
    if isinstance(x, (int, float, np.integer, np.floating)):
        return float(x)
    s = str(x).strip()
    s = s.replace("\u00a0", " ").replace("R$", "").replace(" ", "").replace(".", "").replace(",", ".")
    try:
        return float(s)
    except:
        return np.nan

def norm_sku(x: str) -> str:
    if pd.isna(x):
        return ""
    return unidecode(str(x)).strip().upper()

def exige_colunas(df: pd.DataFrame, obrig: list, nome: str):
    faltam = [c for c in obrig if c not in df.columns]
    if faltam:
        raise ValueError(f"Colunas obrigatÃ³rias ausentes em {nome}: {faltam}")

# ============ Leitura de arquivos ============
def load_any_table(uploaded_file) -> Optional[pd.DataFrame]:
    if uploaded_file is None:
        return None
    name = uploaded_file.name.lower()
    try:
        uploaded_file.seek(0)
        if name.endswith(".csv"):
            df = pd.read_csv(uploaded_file, dtype=str, keep_default_na=False, sep=None, engine="python")
        else:
            df = pd.read_excel(uploaded_file, dtype=str, keep_default_na=False)
    except Exception as e:
        raise RuntimeError(f"NÃ£o consegui ler o arquivo '{uploaded_file.name}': {e}")
    df.columns = [norm_header(c) for c in df.columns]
    if not any("sku" in c for c in df.columns):
        try:
            uploaded_file.seek(0)
            if name.endswith(".csv"):
                df = pd.read_csv(uploaded_file, dtype=str, keep_default_na=False, header=2)
            else:
                df = pd.read_excel(uploaded_file, dtype=str, keep_default_na=False, header=2)
            df.columns = [norm_header(c) for c in df.columns]
        except Exception:
            pass
    sku_col = next((c for c in ["sku", "codigo", "codigo_sku"] if c in df.columns), None)
    if sku_col:
        df[sku_col] = df[sku_col].map(norm_sku)
        df = df[df[sku_col] != ""]
    return df.reset_index(drop=True)

def load_any_table_from_bytes(file_name: str, blob: bytes) -> pd.DataFrame:
    bio = io.BytesIO(blob)
    name = (file_name or "").lower()
    try:
        if name.endswith(".csv"):
            df = pd.read_csv(bio, dtype=str, keep_default_na=False, sep=None, engine="python")
        else:
            df = pd.read_excel(bio, dtype=str, keep_default_na=False)
    except Exception as e:
        raise RuntimeError(f"NÃ£o consegui ler o arquivo salvo '{file_name}': {e}")
    df.columns = [norm_header(c) for c in df.columns]
    if not any("sku" in c for c in df.columns):
        try:
            bio.seek(0)
            if name.endswith(".csv"):
                df = pd.read_csv(bio, dtype=str, keep_default_na=False, header=2)
            else:
                df = pd.read_excel(bio, dtype=str, keep_default_na=False, header=2)
            df.columns = [norm_header(c) for c in df.columns]
        except Exception:
            pass
    sku_col = next((c for c in ["sku", "codigo", "codigo_sku"] if c in df.columns), None)
    if sku_col:
        df[sku_col] = df[sku_col].map(norm_sku)
        df = df[df[sku_col] != ""]
    return df.reset_index(drop=True)

# ============ PadrÃ£o KITS/CAT ============
@dataclass
class Catalogo:
    catalogo_simples: pd.DataFrame  # component_sku, fornecedor, status_reposicao
    kits_reais: pd.DataFrame        # kit_sku, component_sku, qty

def _carregar_padrao_de_content(content: bytes) -> "Catalogo":
    xls = pd.ExcelFile(io.BytesIO(content))

    def load_sheet(opts):
        for n in opts:
            if n in xls.sheet_names:
                return pd.read_excel(xls, n, dtype=str, keep_default_na=False)
        raise RuntimeError(f"Aba nÃ£o encontrada. Esperado uma de {opts}. Abas: {xls.sheet_names}")

    df_kits = load_sheet(["KITS", "KITS_REAIS", "kits", "kits_reais"]).copy()
    df_cat = load_sheet(["CATALOGO_SIMPLES", "CATALOGO", "catalogo_simples", "catalogo"]).copy()

    # KITS
    df_kits = normalize_cols(df_kits)
    m = {}
    for alvo, cand in {
        "kit_sku": ["kit_sku", "kit", "sku_kit"],
        "component_sku": ["component_sku", "componente", "sku_componente", "component"],
        "qty": ["qty", "qtd", "quantidade", "qty_por_kit", "qtd_por_kit", "quantidade_por_kit"],
    }.items():
        for c in cand:
            if c in df_kits.columns:
                m[c] = alvo
                break
    df_kits = df_kits.rename(columns=m)
    exige_colunas(df_kits, ["kit_sku", "component_sku", "qty"], "KITS")
    df_kits = df_kits[["kit_sku", "component_sku", "qty"]].copy()
    df_kits["kit_sku"] = df_kits["kit_sku"].map(norm_sku)
    df_kits["component_sku"] = df_kits["component_sku"].map(norm_sku)
    df_kits["qty"] = df_kits["qty"].map(br_to_float).fillna(0).astype(int)
    df_kits = df_kits[df_kits["qty"] >= 1].drop_duplicates(subset=["kit_sku", "component_sku"])

    # CATALOGO
    df_cat = normalize_cols(df_cat)
    m = {}
    for alvo, cand in {
        "component_sku": ["component_sku", "sku", "produto", "item", "codigo", "sku_componente"],
        "fornecedor": ["fornecedor", "supplier", "fab", "marca"],
        "status_reposicao": ["status_reposicao", "status", "reposicao_status"],
    }.items():
        for c in cand:
            if c in df_cat.columns:
                m[c] = alvo
                break
    df_cat = df_cat.rename(columns=m)
    if "component_sku" not in df_cat.columns:
        raise ValueError("CATALOGO precisa da coluna 'component_sku'.")
    if "fornecedor" not in df_cat.columns:
        df_cat["fornecedor"] = ""
    if "status_reposicao" not in df_cat.columns:
        df_cat["status_reposicao"] = ""
    df_cat["component_sku"] = df_cat["component_sku"].map(norm_sku)
    df_cat["fornecedor"] = df_cat["fornecedor"].fillna("")
    df_cat["status_reposicao"] = df_cat["status_reposicao"].fillna("")
    df_cat = df_cat.drop_duplicates(subset=["component_sku"], keep="last")

    return Catalogo(df_cat, df_kits)

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
        kits = pd.concat([kits, pd.DataFrame(alias, columns=["kit_sku", "component_sku", "qty"])], ignore_index=True)
    kits = kits.drop_duplicates(subset=["kit_sku", "component_sku"])
    return kits

# ============ Mapear tipos ============
def mapear_tipo(df: pd.DataFrame) -> str:
    cols = [c.lower() for c in df.columns]

    tem_sku = any("sku" in c for c in cols)
    tem_v60 = any(c.startswith("vendas_60d") or c in {"vendas 60d", "vendas_qtd_60d"} for c in cols)
    tem_estoque_full = any(("estoque" in c and "full" in c) or c == "estoque_full" for c in cols)
    tem_transito = any(("transito" in c) or c in {"em_transito", "em transito", "em_transito_full"} for c in cols)
    tem_estoque_generico = any(c in {"estoque_atual", "qtd", "quantidade"} or "estoque" in c for c in cols)
    tem_preco = any(c in {"preco", "preco_compra", "custo", "custo_medio", "preco_medio"} for c in cols)

    if tem_sku and (tem_v60 or tem_estoque_full or tem_transito):
        return "FULL"
    if tem_sku and tem_estoque_generico and tem_preco:
        return "FISICO"
    if tem_sku and not tem_preco:
        return "VENDAS"
    return "DESCONHECIDO"

def mapear_colunas(df: pd.DataFrame, tipo: str) -> pd.DataFrame:
    if tipo == "FULL":
        if "sku" in df.columns:
            df["SKU"] = df["sku"].map(norm_sku)
        elif "codigo" in df.columns:
            df["SKU"] = df["codigo"].map(norm_sku)
        elif "codigo_sku" in df.columns:
            df["SKU"] = df["codigo_sku"].map(norm_sku)
        else:
            raise RuntimeError("FULL invÃ¡lido: precisa de SKU/codigo.")
        c_v = [c for c in df.columns if c in ["vendas_qtd_60d", "vendas_60d", "vendas 60d"] or c.startswith("vendas_60d")]
        if not c_v:
            raise RuntimeError("FULL invÃ¡lido: faltou Vendas_60d.")
        df["Vendas_Qtd_60d"] = df[c_v[0]].map(br_to_float).fillna(0).astype(int)
        c_e = [c for c in df.columns if c in ["estoque_full", "estoque_atual"] or ("estoque" in c and "full" in c)]
        if not c_e:
            raise RuntimeError("FULL invÃ¡lido: faltou Estoque_Full.")
        df["Estoque_Full"] = df[c_e[0]].map(br_to_float).fillna(0).astype(int)
        c_t = [c for c in df.columns if c in ["em_transito", "em transito", "em_transito_full"] or ("transito" in c)]
        df["Em_Transito"] = df[c_t[0]].map(br_to_float).fillna(0).astype(int) if c_t else 0
        return df[["SKU", "Vendas_Qtd_60d", "Estoque_Full", "Em_Transito"]].copy()

    if tipo == "FISICO":
        sku_series = (
            df["sku"] if "sku" in df.columns else
            (df["codigo"] if "codigo" in df.columns else
             (df["codigo_sku"] if "codigo_sku" in df.columns else None))
        )
        if sku_series is None:
            cand = next((c for c in df.columns if "sku" in c.lower()), None)
            if cand is None:
                raise RuntimeError("FÃSICO invÃ¡lido: nÃ£o achei SKU.")
            sku_series = df[cand]
        df["SKU"] = sku_series.map(norm_sku)
        c_q = [c for c in df.columns if c in ["estoque_atual", "qtd", "quantidade"] or ("estoque" in c)]
        if not c_q:
            raise RuntimeError("FÃSICO invÃ¡lido: faltou Estoque.")
        df["Estoque_Fisico"] = df[c_q[0]].map(br_to_float).fillna(0).astype(int)
        c_p = [c for c in df.columns if c in ["preco", "preco_compra", "custo", "custo_medio", "preco_medio", "preco_unitario"]]
        if not c_p:
            raise RuntimeError("FÃSICO invÃ¡lido: faltou PreÃ§o/Custo.")
        df["Preco"] = df[c_p[0]].map(br_to_float).fillna(0.0)
        return df[["SKU", "Estoque_Fisico", "Preco"]].copy()

    if tipo == "VENDAS":
        sku_col = next((c for c in df.columns if "sku" in c.lower()), None)
        if not sku_col:
            raise RuntimeError("VENDAS invÃ¡lido: nÃ£o achei SKU.")
        df["SKU"] = df[sku_col].map(norm_sku)
        cand_qty = []
        for c in df.columns:
            cl = c.lower()
            score = 0
            if "qtde" in cl:
                score += 3
            if "quant" in cl:
                score += 2
            if "venda" in cl:
                score += 1
            if "order" in cl:
                score += 1
            if score > 0:
                cand_qty.append((score, c))
        if not cand_qty:
            raise RuntimeError("VENDAS invÃ¡lido: nÃ£o achei Quantidade.")
        cand_qty.sort(reverse=True)
        qcol = cand_qty[0][1]
        df["Quantidade"] = df[qcol].map(br_to_float).fillna(0).astype(int)
        return df[["SKU", "Quantidade"]].copy()

    raise RuntimeError("Tipo desconhecido.")

# ============ ExplosÃ£o por KITS ============
def explodir_por_kits(df: pd.DataFrame, kits: pd.DataFrame, sku_col: str, qtd_col: str) -> pd.DataFrame:
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

# ============ Compra AutomÃ¡tica ============
def calcular(full_df, fisico_df, vendas_df, cat: "Catalogo", h=60, g=0.0, LT=0):
    kits = construir_kits_efetivo(cat)

    full = full_df.copy()
    full["SKU"] = full["SKU"].map(norm_sku)
    full["Vendas_Qtd_60d"] = full["Vendas_Qtd_60d"].astype(int)
    full["Estoque_Full"] = full["Estoque_Full"].astype(int)
    full["Em_Transito"] = full["Em_Transito"].astype(int)

    shp = vendas_df.copy()
    shp["SKU"] = shp["SKU"].map(norm_sku)
    shp["Quantidade_60d"] = shp["Quantidade"].astype(int)

    ml_comp = explodir_por_kits(
        full[["SKU", "Vendas_Qtd_60d"]].rename(columns={"SKU": "kit_sku", "Vendas_Qtd_60d": "Qtd"}),
        kits, "kit_sku", "Qtd"
    ).rename(columns={"Quantidade": "ML_60d"})
    shopee_comp = explodir_por_kits(
        shp[["SKU", "Quantidade_60d"]].rename(columns={"SKU": "kit_sku", "Quantidade_60d": "Qtd"}),
        kits, "kit_sku", "Qtd"
    ).rename(columns={"Quantidade": "Shopee_60d"})

    cat_df = cat.catalogo_simples[["component_sku", "fornecedor", "status_reposicao"]].rename(columns={"component_sku": "SKU"})

    demanda = cat_df.merge(ml_comp, on="SKU", how="left").merge(shopee_comp, on="SKU", how="left")

    # saneamento defensivo
    demanda[["ML_60d", "Shopee_60d"]] = (
        demanda[["ML_60d", "Shopee_60d"]]
        .apply(pd.to_numeric, errors="coerce")
        .fillna(0)
        .clip(lower=0)
        .astype(int)
    )
    demanda["TOTAL_60d"] = np.maximum(
        demanda["ML_60d"] + demanda["Shopee_60d"],
        demanda["ML_60d"]
    ).astype(int)
    demanda["TOTAL_60d"] = demanda["TOTAL_60d"].clip(lower=0)

    fis = fisico_df.copy()
    fis["SKU"] = fis["SKU"].map(norm_sku)
    fis["Estoque_Fisico"] = (
        pd.to_numeric(fis["Estoque_Fisico"], errors="coerce")
        .fillna(0)
        .clip(lower=0)
        .astype(int)
    )
    fis["Preco"] = pd.to_numeric(fis["Preco"], errors="coerce").fillna(0.0)

    base = demanda.merge(fis, on="SKU", how="left")
    base["Estoque_Fisico"] = base["Estoque_Fisico"].fillna(0).astype(int)
    base["Preco"] = base["Preco"].fillna(0.0)

    fator = (1.0 + g / 100.0) ** (h / 30.0)
    fk = full.copy()
    fk["vendas_dia"] = fk["Vendas_Qtd_60d"] / 60.0
    fk["alvo"] = np.round(fk["vendas_dia"] * (LT + h) * fator).astype(int)
    fk["oferta"] = (fk["Estoque_Full"] + fk["Em_Transito"]).astype(int)
    fk["envio_desejado"] = (fk["alvo"] - fk["oferta"]).clip(lower=0).astype(int)

    necessidade = explodir_por_kits(
        fk[["SKU", "envio_desejado"]].rename(columns={"SKU": "kit_sku", "envio_desejado": "Qtd"}),
        kits, "kit_sku", "Qtd"
    ).rename(columns={"Quantidade": "Necessidade"})

    base = base.merge(necessidade, on="SKU", how="left")
    base["Necessidade"] = base["Necessidade"].fillna(0).astype(int)

    base["Demanda_dia"] = base["TOTAL_60d"] / 60.0
    base["Reserva_30d"] = np.round(base["Demanda_dia"] * 30).astype(int)
    base["Folga_Fisico"] = (base["Estoque_Fisico"] - base["Reserva_30d"]).clip(lower=0).astype(int)

    base["Compra_Sugerida"] = (base["Necessidade"] - base["Folga_Fisico"]).clip(lower=0).astype(int)

    # nao_repor some
    mask_nao = base["status_reposicao"].str.lower().str.contains("nao_repor", na=False)
    base.loc[mask_nao, "Compra_Sugerida"] = 0
    base = base[~mask_nao]

    base["Valor_Compra_R$"] = (base["Compra_Sugerida"].astype(float) * base["Preco"].astype(float)).round(2)

    base["Vendas_h_ML"] = np.maximum(0, np.round(base["ML_60d"] * (h / 60.0))).astype(int)
    base["Vendas_h_Shopee"] = np.maximum(0, np.round(base["Shopee_60d"] * (h / 60.0))).astype(int)

    base = base.sort_values(["fornecedor", "Valor_Compra_R$", "SKU"], ascending=[True, False, True])

    df_final = base[[
        "SKU", "fornecedor",
        "Vendas_h_ML", "Vendas_h_Shopee",
        "Estoque_Fisico", "Preco", "Compra_Sugerida", "Valor_Compra_R$",
        "ML_60d", "Shopee_60d", "TOTAL_60d", "Reserva_30d", "Folga_Fisico", "Necessidade"
    ]].reset_index(drop=True)

    fis_unid = int(fis["Estoque_Fisico"].sum())
    fis_valor = float((fis["Estoque_Fisico"] * fis["Preco"]).sum())
    full_unid = int(full["Estoque_Full"].sum())
    comp_full = explodir_por_kits(
        full[["SKU", "Estoque_Full"]].rename(columns={"SKU": "kit_sku", "Estoque_Full": "Qtd"}),
        kits, "kit_sku", "Qtd"
    ).merge(fis[["SKU", "Preco"]], on="SKU", how="left")
    full_valor = float((comp_full["Quantidade"].fillna(0) * comp_full["Preco"].fillna(0.0)).sum())

    painel = {"full_unid": full_unid, "full_valor": full_valor, "fisico_unid": fis_unid, "fisico_valor": fis_valor}
    return df_final, painel

# ============ Export XLSX ============
def sha256_of_csv(df: pd.DataFrame) -> str:
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    return hashlib.sha256(csv_bytes).hexdigest()

def exportar_xlsx(df_final: pd.DataFrame, h: int, params: dict) -> bytes:
    int_cols = [
        "Vendas_h_ML", "Vendas_h_Shopee", "Estoque_Fisico", "Compra_Sugerida",
        "Reserva_30d", "Folga_Fisico", "Necessidade", "ML_60d", "Shopee_60d", "TOTAL_60d"
    ]
    for c in int_cols:
        if c in df_final.columns:
            df_final[c] = (
                pd.to_numeric(df_final[c], errors="coerce")
                .fillna(0)
                .clip(lower=0)
                .astype(int)
            )
    df_final["Valor_Compra_R$"] = (
        df_final["Compra_Sugerida"].astype(float) * df_final["Preco"].astype(float)
    ).round(2)

    for c in int_cols:
        if not np.all(df_final[c].fillna(0).astype(float) >= 0):
            raise RuntimeError(f"Auditoria: coluna '{c}' precisa ser >= 0.")

    calc = (df_final["Compra_Sugerida"] * df_final["Preco"]).round(2).values
    if not np.allclose(df_final["Valor_Compra_R$"].values, calc):
        raise RuntimeError("Auditoria: 'Valor_Compra_R$' != 'Compra_Sugerida x Preco'.")

    hash_str = sha256_of_csv(df_final)
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as w:
        lista = df_final[df_final["Compra_Sugerida"] > 0].copy()
        lista.to_excel(w, sheet_name="Lista_Final", index=False)
        ws = w.sheets["Lista_Final"]
        for i, col in enumerate(lista.columns):
            width = max(12, int(lista[col].astype(str).map(len).max()) + 2)
            ws.set_column(i, i, min(width, 40))
        ws.freeze_panes(1, 0)
        ws.autofilter(0, 0, len(lista), len(lista.columns) - 1)

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

# ================== Sidebar ==================
with st.sidebar:
    st.subheader("ParÃ¢metros")
    h = st.selectbox("Horizonte (dias)", [30, 60, 90], index=1)
    g = st.number_input("Crescimento % ao mÃªs", value=0.0, step=1.0)
    LT = st.number_input("Lead time (dias)", value=0, step=1, min_value=0)

    st.markdown("---")
    st.subheader("PadrÃ£o (KITS/CAT) â€” Google Sheets")
    colA, colB = st.columns([1, 1])
    with colA:
        if st.button("Carregar padrÃ£o agora", use_container_width=True):
            try:
                content = baixar_xlsx_do_sheets(DEFAULT_SHEET_ID)
                cat = _carregar_padrao_de_content(content)
                st.session_state.catalogo_df = cat.catalogo_simples.rename(columns={"component_sku": "sku"})
                st.session_state.kits_df = cat.kits_reais
                st.session_state.loaded_at = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                st.success("PadrÃ£o carregado.")
            except Exception as e:
                st.session_state.catalogo_df = None
                st.session_state.kits_df = None
                st.error(str(e))
    with colB:
        st.link_button("Abrir no Drive (editar)", DEFAULT_SHEET_LINK, use_container_width=True)

# ================== TÃ­tulo ==================
st.title("Reposicao Logistica - Alivvia")
c1, c2 = st.columns([4, 1])
with c2:
    st.markdown(f"<div style='text-align:right; font-size:12px; color:#888;'>Versao: <b>{VERSION}</b></div>", unsafe_allow_html=True)

if st.session_state.catalogo_df is None or st.session_state.kits_df is None:
    st.warning("â–º Carregue o PadrÃ£o (KITS/CAT) no sidebar antes de usar as abas.")

tab1, tab2, tab3, tab4 = st.tabs(["Dados das Empresas", "Compra Automatica", "Alocacao de Compra", "Ordem de Compra"])

# ================== TAB 1: Dados ==================
with tab1:
    st.subheader("Uploads fixos por empresa (salvos; permanecem apÃ³s F5)")

    def bloco_empresa(emp: str):
        st.markdown(f"### {emp}")
        c1, c2 = st.columns(2)
        # FULL
        with c1:
            st.markdown(f"**FULL â€” {emp}**")
            up = st.file_uploader("CSV/XLSX/XLS", type=["csv", "xlsx", "xls"], key=f"up_full_{emp}")
            if up is not None:
                blob = up.read()
                st.session_state[emp]["FULL"] = {"name": up.name, "bytes": blob}
                _store_put(emp, "FULL", up.name, blob)
                st.success(f"FULL salvo: {up.name}")
            it = st.session_state[emp]["FULL"]
            if it["name"]:
                st.markdown(badge_ok("FULL salvo", it["name"]), unsafe_allow_html=True)
                if st.button("Limpar FULL (somente este)", key=f"clr_{emp}_FULL", use_container_width=True):
                    _store_delete(emp, "FULL")
                    st.info("FULL removido.")
        # VENDAS
        with c2:
            st.markdown(f"**Shopee/MT â€” {emp}**")
            up = st.file_uploader("CSV/XLSX/XLS", type=["csv", "xlsx", "xls"], key=f"up_vendas_{emp}")
            if up is not None:
                blob = up.read()
                st.session_state[emp]["VENDAS"] = {"name": up.name, "bytes": blob}
                _store_put(emp, "VENDAS", up.name, blob)
                st.success(f"Vendas salvo: {up.name}")
            it = st.session_state[emp]["VENDAS"]
            if it["name"]:
                st.markdown(badge_ok("Vendas salvo", it["name"]), unsafe_allow_html=True)
                if st.button("Limpar Vendas (somente este)", key=f"clr_{emp}_VENDAS", use_container_width=True):
                    _store_delete(emp, "VENDAS")
                    st.info("Vendas removido.")

        # ESTOQUE
        st.markdown("**Estoque FÃ­sico â€” opcional**")
        up = st.file_uploader("CSV/XLSX/XLS", type=["csv", "xlsx", "xls"], key=f"up_est_{emp}")
        if up is not None:
            blob = up.read()
            st.session_state[emp]["ESTOQUE"] = {"name": up.name, "bytes": blob}
            _store_put(emp, "ESTOQUE", up.name, blob)
            st.success(f"Estoque salvo: {up.name}")
        it = st.session_state[emp]["ESTOQUE"]
        if it["name"]:
            st.markdown(badge_ok("Estoque salvo", it["name"]), unsafe_allow_html=True)
            if st.button("Limpar Estoque (somente este)", key=f"clr_{emp}_ESTOQUE", use_container_width=True):
                _store_delete(emp, "ESTOQUE")
                st.info("Estoque removido.")

        colx, coly = st.columns(2)
        with colx:
            if st.button(f"Salvar {emp}", use_container_width=True, key=f"save_{emp}"):
                for kind in ["FULL", "VENDAS", "ESTOQUE"]:
                    it = st.session_state[emp][kind]
                    if it["name"] and it["bytes"]:
                        _disk_put(emp, kind, it["name"], it["bytes"])
                st.success(f"{emp}: arquivos confirmados (disco).")
        with coly:
            if st.button(f"Limpar {emp}", use_container_width=True, key=f"clr_all_{emp}"):
                st.session_state[emp] = {"FULL": {"name": None, "bytes": None},
                                         "VENDAS": {"name": None, "bytes": None},
                                         "ESTOQUE": {"name": None, "bytes": None}}
                _store_clear(emp)
                st.info(f"{emp} limpo.")

        st.divider()

    bloco_empresa("ALIVVIA")
    bloco_empresa("JCA")

# ================== TAB 2: Compra AutomÃ¡tica ==================
with tab2:
    st.subheader("Gerar Compra (por empresa) â€” lÃ³gica original")

    if st.session_state.catalogo_df is None or st.session_state.kits_df is None:
        st.info("Carregue o PadrÃ£o (KITS/CAT) no sidebar.")
    else:
        empresa = st.radio("Empresa ativa", ["ALIVVIA", "JCA"], horizontal=True, key="empresa_ca")
        dados = st.session_state[empresa]

        col = st.columns(3)
        col[0].info(f"FULL: {dados['FULL']['name'] or 'â€”'}")
        col[1].info(f"Shopee/MT: {dados['VENDAS']['name'] or 'â€”'}")
        col[2].info(f"Estoque: {dados['ESTOQUE']['name'] or 'â€”'}")

        if st.button(f"Gerar Compra â€” {empresa}", type="primary", key=f"btn_calc_{empresa}"):
            try:
                for k, rot in [("FULL", "FULL"), ("VENDAS", "Shopee/MT"), ("ESTOQUE", "Estoque")]:
                    if not (dados[k]["name"] and dados[k]["bytes"]):
                        raise RuntimeError(f"Arquivo '{rot}' nÃ£o foi salvo para {empresa}. Use a aba Dados das Empresas.")

                full_raw = load_any_table_from_bytes(dados["FULL"]["name"], dados["FULL"]["bytes"])
                vendas_raw = load_any_table_from_bytes(dados["VENDAS"]["name"], dados["VENDAS"]["bytes"])
                fisico_raw = load_any_table_from_bytes(dados["ESTOQUE"]["name"], dados["ESTOQUE"]["bytes"])

                t_full = mapear_tipo(full_raw)
                t_v = mapear_tipo(vendas_raw)
                t_f = mapear_tipo(fisico_raw)
                if t_full != "FULL":
                    raise RuntimeError("FULL invÃ¡lido.")
                if t_v != "VENDAS":
                    raise RuntimeError("Vendas invÃ¡lido.")
                if t_f != "FISICO":
                    raise RuntimeError("Estoque invÃ¡lido.")

                full_df = mapear_colunas(full_raw, t_full)
                vendas_df = mapear_colunas(vendas_raw, t_v)
                fisico_df = mapear_colunas(fisico_raw, t_f)

                cat = Catalogo(
                    catalogo_simples=st.session_state.catalogo_df.rename(columns={"sku": "component_sku"}),
                    kits_reais=st.session_state.kits_df
                )
                df_final, painel = calcular(full_df, fisico_df, vendas_df, cat, h=h, g=g, LT=LT)

                st.session_state["resultado_compra"][empresa] = {"df": df_final, "painel": painel}
                st.success("CÃ¡lculo concluÃ­do e salvo. Aplique filtros abaixo.")
            except Exception as e:
                st.error(str(e))

        # Mostrar resultado salvo + filtros (sem recalcular)
        if empresa in st.session_state["resultado_compra"]:
            pkg = st.session_state["resultado_compra"][empresa]
            df_final = pkg["df"]
            painel = pkg["painel"]

            cA, cB, cC, cD = st.columns(4)
            cA.metric("Full (un)", f"{painel['full_unid']:,}".replace(",", "."))
            cB.metric("Full (R$)", f"R$ {painel['full_valor']:,.2f}")
            cC.metric("FÃ­sico (un)", f"{painel['fisico_unid']:,}".replace(",", "."))
            cD.metric("FÃ­sico (R$)", f"R$ {painel['fisico_valor']:,.2f}")

            # >>> NOVO: filtros com busca de SKU por substring
            with st.expander("Filtros (apÃ³s geraÃ§Ã£o) â€” sem recÃ¡lculo", expanded=True):
                fornecedores = sorted([f for f in df_final["fornecedor"].dropna().astype(str).unique().tolist() if f != ""])
                sel_fornec = st.multiselect("Fornecedor", options=fornecedores, default=[], key=f"filtro_fornec_{empresa}")

                sku_all = sorted(df_final["SKU"].dropna().astype(str).unique().tolist())
                txt = st.text_input("Pesquisar SKU (digite parte do cÃ³digo)", key=f"busca_sku_{empresa}", placeholder="ex.: YOGA, 123, PRETOâ€¦")
                if txt:
                    sku_filtrado = [s for s in sku_all if txt.upper() in s.upper()]
                else:
                    sku_filtrado = sku_all

                sel_skus = st.multiselect("Selecione SKUs", options=sku_filtrado, default=[], key=f"filtro_sku_{empresa}")

            df_view = df_final.copy()
            if sel_fornec:
                df_view = df_view[df_view["fornecedor"].isin(sel_fornec)]
            if sel_skus:
                df_view = df_view[df_view["SKU"].isin(sel_skus)]

            # Apenas colunas essenciais
            cols_show = [
                "fornecedor", "SKU",
                "Vendas_h_Shopee", "Vendas_h_ML",
                "Estoque_Fisico", "Preco",
                "Compra_Sugerida", "Valor_Compra_R$",
            ]
            df_view_sub = df_view[[c for c in cols_show if c in df_view.columns]].copy()

            st.caption(f"Linhas apÃ³s filtros: {len(df_view_sub)}")
            st.dataframe(
                df_view_sub,
                use_container_width=True,
                height=500,
                hide_index=True,
                column_config={
                    "fornecedor": st.column_config.TextColumn("Fornecedor"),
                    "SKU": st.column_config.TextColumn("SKU"),
                    "Vendas_h_Shopee": st.column_config.NumberColumn("Vendas (Shopee)", format="%d"),
                    "Vendas_h_ML": st.column_config.NumberColumn("Vendas (FULL)", format="%d"),
                    "Estoque_Fisico": st.column_config.NumberColumn("Estoque FÃ­sico", format="%d"),
                    "Preco": st.column_config.NumberColumn("PreÃ§o", format="R$ %.2f"),
                    "Compra_Sugerida": st.column_config.NumberColumn("Compra Sugerida", format="%d"),
                    "Valor_Compra_R$": st.column_config.NumberColumn("Total (R$)", format="R$ %.2f"),
                },
            )

            colx1, colx2 = st.columns([1, 1])
            with colx1:
                xlsx = exportar_xlsx(df_final, h=h, params={"g": g, "LT": LT, "empresa": empresa})
                st.download_button(
                    "Baixar XLSX (completo)", data=xlsx,
                    file_name=f"Compra_Sugerida_{empresa}_{h}d.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key=f"d_all_{empresa}"
                )
            with colx2:
                xlsx_filtrado = exportar_xlsx(df_view, h=h, params={"g": g, "LT": LT, "empresa": empresa, "filtro": "on"})
                st.download_button(
                    "Baixar XLSX (filtrado)", data=xlsx_filtrado,
                    file_name=f"Compra_Sugerida_{empresa}_{h}d_filtrado.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key=f"d_fil_{empresa}"
                )

            # =============== CONSOLIDADO POR SKU (ALIVVIA + JCA) ===============
            st.markdown("---")
            with st.expander("Consolidado por SKU - ver ALIVVIA e JCA juntos", expanded=False):
                tem_A = "ALIVVIA" in st.session_state["resultado_compra"]
                tem_J = "JCA"     in st.session_state["resultado_compra"]
                if not (tem_A and tem_J):
                    st.info("Gere a compra para ALIVVIA e JCA para habilitar o consolidado.")
                else:
                    dfA = st.session_state["resultado_compra"]["ALIVVIA"]["df"]
                    dfJ = st.session_state["resultado_compra"]["JCA"]["df"]

                    sku_all2 = sorted(set(dfA["SKU"].tolist()) | set(dfJ["SKU"].tolist()))
                    if not sku_all2:
                        st.info("Nenhum SKU disponÃ­vel para consolidado.")
                    else:
                        sku_sel = st.selectbox("Digite/Selecione o SKU", options=sku_all2, index=0, key="consol_sku")

                        def pick(df, sku):
                            row = df.loc[df["SKU"] == sku, ["SKU", "Compra_Sugerida", "Valor_Compra_R$"]]
                            if row.empty:
                                return {"SKU": sku, "Compra_Sugerida": 0, "Valor_Compra_R$": 0.0}
                            r = row.iloc[0].to_dict()
                            r["Compra_Sugerida"] = int(r.get("Compra_Sugerida", 0))
                            r["Valor_Compra_R$"] = float(r.get("Valor_Compra_R$", 0))
                            return r

                        rA = pick(dfA, sku_sel)
                        rJ = pick(dfJ, sku_sel)

                        res = pd.DataFrame([
                            {"Empresa": "ALIVVIA", "SKU": rA["SKU"], "Compra_Sugerida": rA["Compra_Sugerida"], "Valor_Compra_R$": rA["Valor_Compra_R$"]},
                            {"Empresa": "JCA",     "SKU": rJ["SKU"], "Compra_Sugerida": rJ["Compra_Sugerida"], "Valor_Compra_R$": rJ["Valor_Compra_R$"]},
                        ])
                        res["Total_R$"] = res["Valor_Compra_R$"].round(2)

                        st.dataframe(
                            res,
                            use_container_width=True,
                            hide_index=True,
                            column_config={
                                "Empresa": st.column_config.TextColumn("Empresa"),
                                "SKU": st.column_config.TextColumn("SKU"),
                                "Compra_Sugerida": st.column_config.NumberColumn("Compra Sugerida", format="%d"),
                                "Valor_Compra_R$": st.column_config.NumberColumn("Total (R$)", format="R$ %.2f"),
                                "Total_R$": st.column_config.NumberColumn("Total (R$)", format="R$ %.2f"),
                            },
                        )
                        st.success(
                            f"Consolidado para **{sku_sel}** â†’ ALIVVIA: {rA['Compra_Sugerida']} un | JCA: {rJ['Compra_Sugerida']} un"
                        )

            # =============== LISTA COMBINADA (ALIVVIA + JCA) ===============
            st.markdown("---")
            with st.expander("Lista combinada - ver compras das 2 contas lado a lado", expanded=False):
                tem_A = "ALIVVIA" in st.session_state["resultado_compra"]
                tem_J = "JCA"     in st.session_state["resultado_compra"]

                if not (tem_A and tem_J):
                    st.info("Gere a compra para ALIVVIA e JCA para habilitar a lista combinada.")
                else:
                    dfA = st.session_state["resultado_compra"]["ALIVVIA"]["df"][["SKU","fornecedor","Compra_Sugerida","Valor_Compra_R$","Estoque_Fisico","Preco"]].rename(
                        columns={"Compra_Sugerida":"Compra_ALIVVIA","Valor_Compra_R$":"Valor_ALIVVIA","Estoque_Fisico":"Estoque_ALIVVIA","Preco":"Preco_ALIVVIA"}
                    )
                    dfJ = st.session_state["resultado_compra"]["JCA"]["df"][["SKU","fornecedor","Compra_Sugerida","Valor_Compra_R$","Estoque_Fisico","Preco"]].rename(
                        columns={"Compra_Sugerida":"Compra_JCA","Valor_Compra_R$":"Valor_JCA","Estoque_Fisico":"Estoque_JCA","Preco":"Preco_JCA"}
                    )

                    # Unir por SKU (preferir fornecedor de A)
                    dfC = pd.merge(dfA, dfJ, on="SKU", how="outer", suffixes=("_A","_J"))
                    dfC["fornecedor"] = dfC["fornecedor_A"].fillna(dfC["fornecedor_J"])
                    dfC = dfC.drop(columns=["fornecedor_A","fornecedor_J"], errors="ignore")

                    for c in ["Compra_ALIVVIA","Compra_JCA", "Estoque_ALIVVIA","Estoque_JCA"]:
                        if c in dfC.columns:
                            dfC[c] = pd.to_numeric(dfC[c], errors="coerce").fillna(0).astype(int)
                    for c in ["Valor_ALIVVIA","Valor_JCA","Preco_ALIVVIA","Preco_JCA"]:
                        if c in dfC.columns:
                            dfC[c] = pd.to_numeric(dfC[c], errors="coerce").fillna(0.0).astype(float)

                    dfC["Compra_Total"] = dfC["Compra_ALIVVIA"] + dfC["Compra_JCA"]
                    dfC["Valor_Total"]  = (dfC["Valor_ALIVVIA"] + dfC["Valor_JCA"]).round(2)
                    dfC["Estoque_Fisico_Total"] = dfC["Estoque_ALIVVIA"] + dfC["Estoque_JCA"]

                    colf1, colf2, colf3 = st.columns([1,1,2])
                    with colf1:
                        fornecedores_c = sorted([f for f in dfC["fornecedor"].dropna().astype(str).unique().tolist() if f != ""])
                        f_sel = st.multiselect("Fornecedor", fornecedores_c, default=[])
                    with colf2:
                        only_pos = st.checkbox("Somente compra > 0", value=True)
                    with colf3:
                        busca_sku2 = st.text_input("Pesquisar SKU (comb.)", placeholder="parte do SKUâ€¦")

                    dfV = dfC.copy()
                    if f_sel:
                        dfV = dfV[dfV["fornecedor"].isin(f_sel)]
                    if only_pos:
                        dfV = dfV[(dfV["Compra_ALIVVIA"] > 0) | (dfV["Compra_JCA"] > 0)]
                    if busca_sku2:
                        bs = busca_sku2.upper()
                        dfV = dfV[dfV["SKU"].astype(str).str.upper().str.contains(bs)]

                    skus_opts = sorted(dfV["SKU"].astype(str).unique().tolist())
                    skus_sel = st.multiselect("Selecionar SKUs especÃ­ficos (opcional)", options=skus_opts, default=[])
                    if skus_sel:
                        dfV = dfV[dfV["SKU"].isin(skus_sel)]

                    cols_show2 = [
                        "fornecedor","SKU",
                        "Estoque_ALIVVIA","Estoque_JCA","Estoque_Fisico_Total",
                        "Compra_ALIVVIA","Valor_ALIVVIA","Compra_JCA","Valor_JCA",
                        "Compra_Total","Valor_Total"
                    ]
                    dfV = dfV[[c for c in cols_show2 if c in dfV.columns]]

                    st.caption(f"Linhas apÃ³s filtros: {len(dfV)}")
                    st.dataframe(
                        dfV,
                        use_container_width=True,
                        hide_index=True,
                        height=500,
                        column_config={
                            "fornecedor": st.column_config.TextColumn("Fornecedor"),
                            "SKU": st.column_config.TextColumn("SKU"),
                            "Estoque_ALIVVIA": st.column_config.NumberColumn("Estoque ALIVVIA", format="%d"),
                            "Estoque_JCA": st.column_config.NumberColumn("Estoque JCA", format="%d"),
                            "Estoque_Fisico_Total": st.column_config.NumberColumn("Estoque Total", format="%d"),
                            "Compra_ALIVVIA": st.column_config.NumberColumn("Compra ALIVVIA", format="%d"),
                            "Valor_ALIVVIA": st.column_config.NumberColumn("Valor ALIVVIA (R$)", format="R$ %.2f"),
                            "Compra_JCA": st.column_config.NumberColumn("Compra JCA", format="%d"),
                            "Valor_JCA": st.column_config.NumberColumn("Valor JCA (R$)", format="R$ %.2f"),
                            "Compra_Total": st.column_config.NumberColumn("Compra Total", format="%d"),
                            "Valor_Total": st.column_config.NumberColumn("Valor Total (R$)", format="R$ %.2f"),
                        },
                    )

                    # Download XLSX combinado (com tratamento para DF vazio)
                    def _xlsx_combinado(df):
                        import io as _io, pandas as _pd, numpy as _np
                        bio = _io.BytesIO()
                        with _pd.ExcelWriter(bio, engine="xlsxwriter") as w:
                            df.to_excel(w, sheet_name="Compra_2Contas", index=False)
                            ws = w.sheets["Compra_2Contas"]

                            if df.shape[0] == 0:
                                for i in range(len(df.columns)):
                                    ws.set_column(i, i, 14)
                                ws.freeze_panes(1, 0)
                                ws.autofilter(0, 0, 0, max(0, len(df.columns) - 1))
                            else:
                                for i, col in enumerate(df.columns):
                                    s = df[col].astype(str).fillna("")
                                    s = s.replace({"None": "", "nan": "", "NaN": ""})
                                    max_len = s.map(len).max()
                                    if _pd.isna(max_len):
                                        max_len = 0
                                    width = max(12, min(40, int(max_len) + 2))
                                    ws.set_column(i, i, width)

                                ws.freeze_panes(1, 0)
                                ws.autofilter(0, 0, len(df), len(df.columns) - 1)

                        bio.seek(0)
                        return bio.read()

                    xlsx2 = _xlsx_combinado(dfV)
                    st.download_button(
                        "Baixar XLSX (lista combinada)",
                        data=xlsx2,
                        file_name="Compra_Duas_Contas.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True
                    )
        
# ===== NOVO: Selecionar e enviar da Lista Combinada =====
try:
    dfV_edit = dfV.copy()
    dfV_edit["Selecionar"] = False
    dfV_edit = st.data_editor(
        dfV_edit,
        use_container_width=True,
        hide_index=True,
        height=400,
        column_config={
            "Selecionar": st.column_config.CheckboxColumn("Selecionar"),
            "fornecedor": st.column_config.TextColumn("Fornecedor"),
            "SKU": st.column_config.TextColumn("SKU"),
            "Estoque_ALIVVIA": st.column_config.NumberColumn("Estoque ALIVVIA", format="%d"),
            "Estoque_JCA": st.column_config.NumberColumn("Estoque JCA", format="%d"),
            "Estoque_Fisico_Total": st.column_config.NumberColumn("Estoque Total", format="%d"),
            "Compra_ALIVVIA": st.column_config.NumberColumn("Compra ALIVVIA", format="%d"),
            "Valor_ALIVVIA": st.column_config.NumberColumn("Valor ALIVVIA (R$)", format="R$ %.2f"),
            "Compra_JCA": st.column_config.NumberColumn("Compra JCA", format="%d"),
            "Valor_JCA": st.column_config.NumberColumn("Valor JCA (R$)", format="R$ %.2f"),
            "Compra_Total": st.column_config.NumberColumn("Compra Total", format="%d"),
            "Valor_Total": st.column_config.NumberColumn("Valor Total (R$)", format="R$ %.2f"),
        },
    )
    sel_comb = dfV_edit[dfV_edit["Selecionar"] == True].drop(columns=["Selecionar"], errors="ignore")
    col_btn1, col_btn2 = st.columns([1,1])
    with col_btn1:
        if st.button("➕ Enviar p/ OC — ALIVVIA", use_container_width=True):
            base = sel_comb.rename(columns={
                "Compra_ALIVVIA":"Compra_Sugerida",
                "Valor_ALIVVIA":"Valor_Compra_R$",
                "Preco_ALIVVIA":"Preco",
            })

            oc.adicionar_itens_cesta("ALIVVIA", base[["SKU","fornecedor","Preco","Compra_Sugerida","Valor_Compra_R$"]].copy())
    with col_btn2:
        if st.button("➕ Enviar p/ OC — JCA", use_container_width=True):
            base = sel_comb.rename(columns={
                "Compra_JCA":"Compra_Sugerida",
                "Valor_JCA":"Valor_Compra_R$",
                "Preco_JCA":"Preco",
            })

            oc.adicionar_itens_cesta("JCA", base[["SKU","fornecedor","Preco","Compra_Sugerida","Valor_Compra_R$"]].copy())
except Exception as _e2:
    st.info("Seleção combinada para OC aparece após gerar as compras das duas empresas.")
# ===== FIM NOVO =====
else:
            st.info("Clique Gerar Compra para calcular e entÃ£o aplicar filtros.")

# ================== TAB 3: AlocaÃ§Ã£o de Compra ==================
with tab3:
    st.subheader("Distribuir quantidade entre empresas â€” proporcional Ã s vendas (FULL + Shopee)")

    if st.session_state.catalogo_df is None or st.session_state.kits_df is None:
        st.info("Carregue o PadrÃ£o (KITS/CAT) no sidebar.")
    else:
        CATALOGO = st.session_state.catalogo_df
        sku_opcoes = CATALOGO["sku"].dropna().astype(str).sort_values().unique().tolist()
        sku_escolhido = st.selectbox("SKU do componente para alocar", sku_opcoes, key="alloc_sku")
        qtd_lote = st.number_input("Quantidade total do lote", min_value=1, value=1000, step=50)

        st.caption("Necessita FULL e Shopee/MT salvos para ALIVVIA e JCA na aba Dados.")

        if st.button("Calcular alocaÃ§Ã£o proporcional", type="primary"):
            try:
                missing = []
                for emp in ["ALIVVIA", "JCA"]:
                    if not (st.session_state[emp]["FULL"]["name"] and st.session_state[emp]["FULL"]["bytes"]):
                        missing.append(f"{emp} FULL")
                    if not (st.session_state[emp]["VENDAS"]["name"] and st.session_state[emp]["VENDAS"]["bytes"]):
                        missing.append(f"{emp} Shopee/MT")
                if missing:
                    raise RuntimeError("Faltam arquivos salvos: " + ", ".join(missing))

                def read_pair(emp: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
                    fa = load_any_table_from_bytes(st.session_state[emp]["FULL"]["name"], st.session_state[emp]["FULL"]["bytes"])
                    sa = load_any_table_from_bytes(st.session_state[emp]["VENDAS"]["name"], st.session_state[emp]["VENDAS"]["bytes"])
                    tfa = mapear_tipo(fa)
                    tsa = mapear_tipo(sa)
                    if tfa != "FULL":
                        raise RuntimeError(f"FULL invÃ¡lido ({emp}).")
                    if tsa != "VENDAS":
                        raise RuntimeError(f"Vendas invÃ¡lido ({emp}).")
                    return mapear_colunas(fa, tfa), mapear_colunas(sa, tsa)

                full_A, shp_A = read_pair("ALIVVIA")
                full_J, shp_J = read_pair("JCA")

                cat = Catalogo(
                    catalogo_simples=CATALOGO.rename(columns={"sku": "component_sku"}),
                    kits_reais=st.session_state.kits_df
                )
                kits = construir_kits_efetivo(cat)

                def vendas_componente(full_df, shp_df) -> pd.DataFrame:
                    a = explodir_por_kits(
                        full_df[["SKU", "Vendas_Qtd_60d"]].rename(columns={"SKU": "kit_sku", "Vendas_Qtd_60d": "Qtd"}),
                        kits, "kit_sku", "Qtd"
                    ).rename(columns={"Quantidade": "ML_60d"})
                    b = explodir_por_kits(
                        shp_df[["SKU", "Quantidade"]].rename(columns={"SKU": "kit_sku", "Quantidade": "Qtd"}),
                        kits, "kit_sku", "Qtd"
                    ).rename(columns={"Quantidade": "Shopee_60d"})
                    out = pd.merge(a, b, on="SKU", how="outer").fillna(0)
                    out["Demanda_60d"] = out["ML_60d"].astype(int) + out["Shopee_60d"].astype(int)
                    return out[["SKU", "Demanda_60d"]]

                demA = vendas_componente(full_A, shp_A)
                demJ = vendas_componente(full_J, shp_J)

                sku_norm = norm_sku(sku_escolhido)
                dA = int(demA.loc[demA["SKU"] == sku_norm, "Demanda_60d"].sum())
                dJ = int(demJ.loc[demJ["SKU"] == sku_norm, "Demanda_60d"].sum())
                total = dA + dJ

                if total == 0:
                    st.warning("Sem vendas detectadas; alocaÃ§Ã£o 50/50 por falta de base.")
                    propA = propJ = 0.5
                else:
                    propA = dA / total
                    propJ = dJ / total

                alocA = int(round(qtd_lote * propA))
                alocJ = int(qtd_lote - alocA)

                res = pd.DataFrame([
                    {"Empresa": "ALIVVIA", "SKU": sku_norm, "Demanda_60d": dA, "Proporcao": round(propA, 4), "Alocacao_Sugerida": alocA},
                    {"Empresa": "JCA", "SKU": sku_norm, "Demanda_60d": dJ, "Proporcao": round(propJ, 4), "Alocacao_Sugerida": alocJ},
                ])
                st.dataframe(res, use_container_width=True)
                st.success(f"Total alocado: {qtd_lote} un (ALIVVIA {alocA} | JCA {alocJ})")
                st.download_button(
                    "Baixar alocaÃ§Ã£o (.csv)",
                    data=res.to_csv(index=False).encode("utf-8"),
                    file_name=f"Alocacao_{sku_norm}_{qtd_lote}.csv",
                    mime="text/csv"
                )
            except Exception as e:
                st.error(str(e))

# ================== RodapÃ© ==================
st.caption(f"Â© Alivvia â€” {VERSION}")



