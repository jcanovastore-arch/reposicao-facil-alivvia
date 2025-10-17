# Reposição Logística — Alivvia (Streamlit)
# v3.2.1 - fixa:
# 1) Persistência de uploads: salva também no disco (.uploads/) para sobreviver a F5
# 2) Filtros pós-cálculo: não somem o resultado; ficam fora do botão

import io, os, json, re, hashlib, datetime as dt
from dataclasses import dataclass
from typing import Optional, Tuple

import numpy as np
import pandas as pd
from unidecode import unidecode
import streamlit as st
import requests
from requests.adapters import HTTPAdapter, Retry

VERSION = "v3.2.1 - 2025-10-17"

st.set_page_config(page_title="Reposição Logística — Alivvia", layout="wide")

DEFAULT_SHEET_LINK = (
    "https://docs.google.com/spreadsheets/d/1cTLARjq-B5g50dL6tcntg7lb_Iu0ta43/"
    "edit?usp=sharing&ouid=109458533144345974874&rtpof=true&sd=true"
)
DEFAULT_SHEET_ID = "1cTLARjq-B5g50dL6tcntg7lb_Iu0ta43"

# -------------------------- PERSISTÊNCIA EM DISCO --------------------------
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
            # remove apenas arquivos conhecidos
            for root, _, files in os.walk(p):
                for fn in files:
                    try: os.remove(os.path.join(root, fn))
                    except: pass
    except: pass

# --------------------------- COFRE EM MEMÓRIA ------------------------------
@st.cache_resource(show_spinner=False)
def _file_store():
    return {
        "ALIVVIA": {"FULL": None, "VENDAS": None, "ESTOQUE": None},
        "JCA":     {"FULL": None, "VENDAS": None, "ESTOQUE": None},
    }

def _store_put(emp: str, kind: str, name: str, blob: bytes):
    store = _file_store()
    store[emp][kind] = {"name": name, "bytes": blob}
    _disk_put(emp, kind, name, blob)  # grava no disco também

def _store_get(emp: str, kind: str):
    store = _file_store()
    it = store[emp][kind]
    if it: 
        return it
    # se memória vazia, tenta disco
    it = _disk_get(emp, kind)
    if it:
        store[emp][kind] = it
        return it
    return None

def _store_clear(emp: str):
    store = _file_store()
    store[emp] = {"FULL": None, "VENDAS": None, "ESTOQUE": None}
    _disk_clear(emp)

# ----------------------------- ESTADO BÁSICO -------------------------------
def _ensure_state():
    st.session_state.setdefault("catalogo_df", None)
    st.session_state.setdefault("kits_df", None)
    st.session_state.setdefault("loaded_at", None)
    st.session_state.setdefault("alt_sheet_link", DEFAULT_SHEET_LINK)
    st.session_state.setdefault("resultado_compra", {})  # guarda df_final por empresa

    for emp in ["ALIVVIA", "JCA"]:
        st.session_state.setdefault(emp, {})
        for kind in ["FULL", "VENDAS", "ESTOQUE"]:
            st.session_state[emp].setdefault(kind, {"name": None, "bytes": None})
            # se sessão vazia, tenta recuperar do cofre/disco
            if st.session_state[emp][kind]["name"] is None:
                it = _store_get(emp, kind)
                if it:
                    st.session_state[emp][kind] = it

_ensure_state()

# ---------------------------- HTTP / SHEETS --------------------------------
def _requests_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(total=3, backoff_factor=0.6, status_forcelist=[429,500,502,503,504], allowed_methods=["GET"])
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.headers.update({"User-Agent":"Mozilla/5.0"})
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
    r = s.get(url, timeout=30); r.raise_for_status(); return r.content

# ----------------------------- UTILS DE DADOS ------------------------------
def norm_header(s: str) -> str:
    s = (s or "").strip()
    s = unidecode(s).lower()
    for ch in [" ", "-", "(", ")", "/", "\\", "[", "]", ".", ",", ";", ":"]:
        s = s.replace(ch, "_")
    while "__" in s: s = s.replace("__","_")
    return s.strip("_")

def normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy(); df.columns = [norm_header(c) for c in df.columns]; return df

def br_to_float(x):
    if pd.isna(x): return np.nan
    if isinstance(x,(int,float,np.integer,np.floating)): return float(x)
    s = str(x).strip().replace("\u00a0"," ").replace("R$","").replace(" ","").replace(".","").replace(",",".")
    try: return float(s)
    except: return np.nan

def norm_sku(x: str) -> str:
    if pd.isna(x): return ""
    return unidecode(str(x)).strip().upper()

def exige_colunas(df: pd.DataFrame, obrig: list, nome: str):
    faltam = [c for c in obrig if c not in df.columns]
    if faltam: raise ValueError(f"Colunas obrigatórias ausentes em {nome}: {faltam}")

# --------------------------- LEITURA DE ARQUIVOS ---------------------------
def load_any_table(uploaded_file) -> Optional[pd.DataFrame]:
    if uploaded_file is None: return None
    name = uploaded_file.name.lower()
    try:
        uploaded_file.seek(0)
        if name.endswith(".csv"):
            df = pd.read_csv(uploaded_file, dtype=str, keep_default_na=False, sep=None, engine="python")
        else:
            df = pd.read_excel(uploaded_file, dtype=str, keep_default_na=False)
    except Exception as e:
        raise RuntimeError(f"Não consegui ler o arquivo '{uploaded_file.name}': {e}")
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
    sku_col = next((c for c in ["sku","codigo","codigo_sku"] if c in df.columns), None)
    if sku_col:
        df[sku_col] = df[sku_col].map(norm_sku)
        df = df[df[sku_col] != ""]
    return df.reset_index(drop=True)

def load_any_table_from_bytes(file_name: str, blob: bytes) -> pd.DataFrame:
    bio = io.BytesIO(blob); name = (file_name or "").lower()
    try:
        if name.endswith(".csv"):
            df = pd.read_csv(bio, dtype=str, keep_default_na=False, sep=None, engine="python")
        else:
            df = pd.read_excel(bio, dtype=str, keep_default_na=False)
    except Exception as e:
        raise RuntimeError(f"Não consegui ler o arquivo salvo '{file_name}': {e}")
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
    sku_col = next((c for c in ["sku","codigo","codigo_sku"] if c in df.columns), None)
    if sku_col:
        df[sku_col] = df[sku_col].map(norm_sku)
        df = df[df[sku_col] != ""]
    return df.reset_index(drop=True)

# ----------------------------- PADRÃO KITS/CAT -----------------------------
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
        raise RuntimeError(f"Aba não encontrada. Esperado uma de {opts}. Abas: {xls.sheet_names}")

    df_kits = load_sheet(["KITS","KITS_REAIS","kits","kits_reais"]).copy()
    df_cat  = load_sheet(["CATALOGO_SIMPLES","CATALOGO","catalogo_simples","catalogo"]).copy()

    df_kits = normalize_cols(df_kits)
    m = {}
    for alvo, cand in {
        "kit_sku": ["kit_sku","kit","sku_kit"],
        "component_sku": ["component_sku","componente","sku_componente","component"],
        "qty": ["qty","qtd","quantidade","qty_por_kit","qtd_por_kit","quantidade_por_kit"]
    }.items():
        for c in cand:
            if c in df_kits.columns: m[c] = alvo; break
    df_kits = df_kits.rename(columns=m)
    exige_colunas(df_kits, ["kit_sku","component_sku","qty"], "KITS")
    df_kits = df_kits[["kit_sku","component_sku","qty"]].copy()
    df_kits["kit_sku"] = df_kits["kit_sku"].map(norm_sku)
    df_kits["component_sku"] = df_kits["component_sku"].map(norm_sku)
    df_kits["qty"] = df_kits["qty"].map(br_to_float).fillna(0).astype(int)
    df_kits = df_kits[df_kits["qty"] >= 1].drop_duplicates(subset=["kit_sku","component_sku"])

    df_cat = normalize_cols(df_cat)
    m = {}
    for alvo, cand in {
        "component_sku": ["component_sku","sku","produto","item","codigo","sku_componente"],
        "fornecedor": ["fornecedor","supplier","fab","marca"],
        "status_reposicao": ["status_reposicao","status","reposicao_status"]
    }.items():
        for c in cand:
            if c in df_cat.columns: m[c] = alvo; break
    df_cat = df_cat.rename(columns=m)
    if "component_sku" not in df_cat.columns: raise ValueError("CATALOGO precisa da coluna 'component_sku'.")
    if "fornecedor" not in df_cat.columns: df_cat["fornecedor"] = ""
    if "status_reposicao" not in df_cat.columns: df_cat["status_reposicao"] = ""
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
        kits = pd.concat([kits, pd.DataFrame(alias, columns=["kit_sku","component_sku","qty"])], ignore_index=True)
    kits = kits.drop_duplicates(subset=["kit_sku","component_sku"])
    return kits

# ------------------ MAPEAMENTO FULL/FISICO/VENDAS --------------------------
def mapear_tipo(df: pd.DataFrame) -> str:
    cols = [c.lower() for c in df.columns]
    tem_sku = any("sku" in c for c in cols)
    tem_v60 = any(c.startswith("vendas_60d") or c in {"vendas 60d","vendas_qtd_60d"} for c in cols)
    tem_estoque_full = any(("estoque" in c and "full" in c) or c=="estoque_full" for c in cols)
    tem_transito = any(("transito" in c) or c in {"em_transito","em transito","em_transito_full"} for c in cols)
    tem_estoque_generico = any(c in {"estoque_atual","qtd","quantidade"} or "estoque" in c for c in cols)
    tem_preco = any(c in {"preco","preco_compra","custo","custo_medio","preco_medio"} for c in cols)

    if tem_sku and (tem_v60 or tem_estoque_full or tem_transito): return "FULL"
    if tem_sku and tem_estoque_generico and tem_preco: return "FISICO"
    if tem_sku and not tem_preco: return "VENDAS"
    return "DESCONHECIDO"

def mapear_colunas(df: pd.DataFrame, tipo: str) -> pd.DataFrame:
    if tipo == "FULL":
        if "sku" in df.columns: df["SKU"] = df["sku"].map(norm_sku)
        elif "codigo" in df.columns: df["SKU"] = df["codigo"].map(norm_sku)
        elif "codigo_sku" in df.columns: df["SKU"] = df["codigo_sku"].map(norm_sku)
        else: raise RuntimeError("FULL inválido: precisa de SKU/codigo.")
        c_v = [c for c in df.columns if c in ["vendas_qtd_60d","vendas_60d","vendas 60d"] or c.startswith("vendas_60d")]
        if not c_v: raise RuntimeError("FULL inválido: faltou Vendas_60d.")
        df["Vendas_Qtd_60d"] = df[c_v[0]].map(br_to_float).fillna(0).astype(int)
        c_e = [c for c in df.columns if c in ["estoque_full","estoque_atual"] or ("estoque" in c and "full" in c)]
        if not c_e: raise RuntimeError("FULL inválido: faltou Estoque_Full.")
        df["Estoque_Full"] = df[c_e[0]].map(br_to_float).fillna(0).astype(int)
        c_t = [c for c in df.columns if c in ["em_transito","em transito","em_transito_full"] or ("transito" in c)]
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
            if cand is None: raise RuntimeError("FÍSICO inválido: não achei SKU.")
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
        if not sku_col: raise RuntimeError("VENDAS inválido: não achei SKU.")
        df["SKU"] = df[sku_col].map(norm_sku)
        cand_qty = []
        for c in df.columns:
            cl = c.lower(); score = 0
            if "qtde" in cl: score += 3
            if "quant" in cl: score += 2
            if "venda" in cl: score += 1
            if "order" in cl: score += 1
            if score > 0: cand_qty.append((score, c))
        if not cand_qty: raise RuntimeError("VENDAS inválido: não achei Quantidade.")
        cand_qty.sort(reverse=True); qcol = cand_qty[0][1]
        df["Quantidade"] = df[qcol].map(br_to_float).fillna(0).astype(int)
        return df[["SKU","Quantidade"]].copy()

    raise RuntimeError("Tipo desconhecido.")

# ------------------------------ KITS / EXPLODE -----------------------------
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

# --------------------------- COMPRA AUTOMÁTICA -----------------------------
def calcular(full_df, fisico_df, vendas_df, cat: "Catalogo", h=60, g=0.0, LT=0):
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

    # regra original + ocultar
    mask_nao = base["status_reposicao"].str.lower().str.contains("nao_repor", na=False)
    base.loc[mask_nao, "Compra_Sugerida"] = 0
    base = base[~mask_nao]

    base["Valor_Compra_R$"] = (base["Compra_Sugerida"].astype(float) * base["Preco"].astype(float)).round(2)
    base["Vendas_h_ML"]     = np.round(base["ML_60d"] * (h/60.0)).astype(int)
    base["Vendas_h_Shopee"] = np.round(base["Shopee_60d"] * (h/60.0)).astype(int)

    base = base.sort_values(["fornecedor","Valor_Compra_R$","SKU"], ascending=[True, False, True])

    df_final = base[[
        "SKU","fornecedor",
        "Vendas_h_ML","Vendas_h_Shopee",
        "Estoque_Fisico","Preco","Compra_Sugerida","Valor_Compra_R$",
        "ML_60d","Shopee_60d","TOTAL_60d","Reserva_30d","Folga_Fisico","Necessidade"
    ]].reset_index(drop=True)

    fis_unid  = int(fis["Estoque_Fisico"].sum())
    fis_valor = float((fis["Estoque_Fisico"] * fis["Preco"]).sum())
    full_unid = int(full["Estoque_Full"].sum())
    # valor full aproximado usando preço do físico por componente
    comp_full = explodir_por_kits(
        full[["SKU","Estoque_Full"]].rename(columns={"SKU":"kit_sku","Estoque_Full":"Qtd"}),
        kits,"kit_sku","Qtd").merge(fis[["SKU","Preco"]], on="SKU", how="left")
    full_valor = float((comp_full["Quantidade"].fillna(0) * comp_full["Preco"].fillna(0.0)).sum())

    painel = {"full_unid": full_unid, "full_valor": full_valor, "fisico_unid": fis_unid, "fisico_valor": fis_valor}
    return df_final, painel

# ------------------------------ EXPORTAÇÃO ---------------------------------
def sha256_of_csv(df: pd.DataFrame) -> str:
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    return hashlib.sha256(csv_bytes).hexdigest()

def exportar_xlsx(df_final: pd.DataFrame, h: int, params: dict) -> bytes:
    int_cols = ["Vendas_h_ML","Vendas_h_Shopee","Estoque_Fisico","Compra_Sugerida","Reserva_30d","Folga_Fisico","Necessidade","ML_60d","Shopee_60d","TOTAL_60d"]
    for c in int_cols:
        if not np.all(df_final[c].fillna(0).astype(float) >= 0):
            raise RuntimeError(f"Auditoria: coluna '{c}' precisa ser ≥ 0.")

    calc = (df_final["Compra_Sugerida"] * df_final["Preco"]).round(2).values
    if not np.allclose(df_final["Valor_Compra_R$"].values, calc):
        raise RuntimeError("Auditoria: 'Valor_Compra_R$' ≠ 'Compra_Sugerida × Preco'.")

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

# ------------------------------ SIDEBAR ------------------------------------
with st.sidebar:
    st.subheader("Parâmetros")
    h  = st.selectbox("Horizonte (dias)", [30, 60, 90], index=1)
    g  = st.number_input("Crescimento % ao mês", value=0.0, step=1.0)
    LT = st.number_input("Lead time (dias)", value=0, step=1, min_value=0)

    st.markdown("---")
    st.subheader("Padrão (KITS/CAT) — Google Sheets")
    colA, colB = st.columns([1, 1])
    with colA:
        if st.button("Carregar padrão agora", use_container_width=True):
            try:
                content = baixar_xlsx_do_sheets(DEFAULT_SHEET_ID)
                from io import BytesIO
                cat = _carregar_padrao_de_content(content)
                st.session_state.catalogo_df = cat.catalogo_simples.rename(columns={"component_sku":"sku"})
                st.session_state.kits_df = cat.kits_reais
                st.session_state.loaded_at = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                st.success("Padrão carregado.")
            except Exception as e:
                st.session_state.catalogo_df = None; st.session_state.kits_df = None
                st.error(str(e))
    with colB:
        st.link_button("🔗 Abrir no Drive (editar)", DEFAULT_SHEET_LINK, use_container_width=True)

# ------------------------------ TÍTULO -------------------------------------
st.title("Reposição Logística — Alivvia")
c1, c2 = st.columns([4,1])
with c2:
    st.markdown(f"<div style='text-align:right; font-size:12px; color:#888;'>Versão: <b>{VERSION}</b></div>", unsafe_allow_html=True)

if st.session_state.catalogo_df is None or st.session_state.kits_df is None:
    st.warning("► Carregue o **Padrão (KITS/CAT)** no sidebar antes de usar as abas.")

tab1, tab2, tab3 = st.tabs(["📂 Dados das Empresas", "🧮 Compra Automática", "📦 Alocação de Compra"])

# ------------------------------ TAB 1 --------------------------------------
with tab1:
    st.subheader("Uploads fixos por empresa (salvos; permanecem após F5)")

    def bloco_empresa(emp: str):
        st.markdown(f"### {emp}")
        c1, c2 = st.columns(2)
        # FULL
        with c1:
            st.markdown(f"**FULL — {emp}**")
            up = st.file_uploader("CSV/XLSX/XLS", type=["csv","xlsx","xls"], key=f"up_full_{emp}")
            if up is not None:
                blob = up.read()
                st.session_state[emp]["FULL"] = {"name": up.name, "bytes": blob}
                _store_put(emp, "FULL", up.name, blob)
                st.success(f"FULL salvo: {up.name}")
            it = st.session_state[emp]["FULL"]
            if it["name"]: st.caption(f"FULL salvo: **{it['name']}**")
        # VENDAS
        with c2:
            st.markdown(f"**Shopee/MT — {emp}**")
            up = st.file_uploader("CSV/XLSX/XLS", type=["csv","xlsx","xls"], key=f"up_vendas_{emp}")
            if up is not None:
                blob = up.read()
                st.session_state[emp]["VENDAS"] = {"name": up.name, "bytes": blob}
                _store_put(emp, "VENDAS", up.name, blob)
                st.success(f"Vendas salvo: {up.name}")
            it = st.session_state[emp]["VENDAS"]
            if it["name"]: st.caption(f"Vendas salvo: **{it['name']}**")

        # ESTOQUE
        st.markdown("**Estoque Físico — opcional**")
        up = st.file_uploader("CSV/XLSX/XLS", type=["csv","xlsx","xls"], key=f"up_est_{emp}")
        if up is not None:
            blob = up.read()
            st.session_state[emp]["ESTOQUE"] = {"name": up.name, "bytes": blob}
            _store_put(emp, "ESTOQUE", up.name, blob)
            st.success(f"Estoque salvo: {up.name}")
        it = st.session_state[emp]["ESTOQUE"]
        if it["name"]: st.caption(f"Estoque salvo: **{it['name']}**")

        colx, coly = st.columns(2)
        with colx:
            if st.button(f"Salvar {emp}", use_container_width=True, key=f"save_{emp}"):
                # já salvamos ao fazer upload; aqui forçamos a persistência para o disco
                for kind in ["FULL","VENDAS","ESTOQUE"]:
                    it = st.session_state[emp][kind]
                    if it["name"] and it["bytes"]:
                        _disk_put(emp, kind, it["name"], it["bytes"])
                st.success(f"{emp}: arquivos confirmados (disco).")
        with coly:
            if st.button(f"Limpar {emp}", use_container_width=True, key=f"clr_{emp}"):
                st.session_state[emp] = {"FULL":{"name":None,"bytes":None},
                                         "VENDAS":{"name":None,"bytes":None},
                                         "ESTOQUE":{"name":None,"bytes":None}}
                _store_clear(emp)
                st.info(f"{emp} limpo.")

        st.divider()

    bloco_empresa("ALIVVIA")
    bloco_empresa("JCA")

# ------------------------------ TAB 2 --------------------------------------
with tab2:
    st.subheader("Gerar Compra (por empresa) — lógica original")

    if st.session_state.catalogo_df is None or st.session_state.kits_df is None:
        st.info("Carregue o **Padrão (KITS/CAT)** no sidebar.")
    else:
        empresa = st.radio("Empresa ativa", ["ALIVVIA", "JCA"], horizontal=True, key="empresa_ca")
        dados = st.session_state[empresa]

        col = st.columns(3)
        col[0].info(f"FULL: {dados['FULL']['name'] or '—'}")
        col[1].info(f"Shopee/MT: {dados['VENDAS']['name'] or '—'}")
        col[2].info(f"Estoque: {dados['ESTOQUE']['name'] or '—'}")

        # ---------------- BOTÃO: GERA E SALVA RESULTADO ----------------
        if st.button(f"Gerar Compra — {empresa}", type="primary", key=f"btn_calc_{empresa}"):
            try:
                for k, rot in [("FULL","FULL"),("VENDAS","Shopee/MT"),("ESTOQUE","Estoque")]:
                    if not (dados[k]["name"] and dados[k]["bytes"]):
                        raise RuntimeError(f"Arquivo '{rot}' não foi salvo para {empresa}. Use a aba **Dados das Empresas**.")
                full_raw   = load_any_table_from_bytes(dados["FULL"]["name"],   dados["FULL"]["bytes"])
                vendas_raw = load_any_table_from_bytes(dados["VENDAS"]["name"], dados["VENDAS"]["bytes"])
                fisico_raw = load_any_table_from_bytes(dados["ESTOQUE"]["name"],dados["ESTOQUE"]["bytes"])

                t_full = mapear_tipo(full_raw); t_v = mapear_tipo(vendas_raw); t_f = mapear_tipo(fisico_raw)
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
                df_final, painel = calcular(full_df, fisico_df, vendas_df, cat, h=h, g=g, LT=LT)

                # salva resultado para uso APÓS o clique (filtros não recalculam)
                st.session_state["resultado_compra"][empresa] = {"df": df_final, "painel": painel}
                st.success("Cálculo concluído e salvo. Aplique filtros abaixo.")
            except Exception as e:
                st.error(str(e))

        # ---------------- MOSTRAR RESULTADO SALVO + FILTROS -------------
        if empresa in st.session_state["resultado_compra"]:
            pkg = st.session_state["resultado_compra"][empresa]
            df_final = pkg["df"]; painel = pkg["painel"]

            cA, cB, cC, cD = st.columns(4)
            cA.metric("Full (un)",  f"{painel['full_unid']:,}".replace(",", "."))
            cB.metric("Full (R$)",  f"R$ {painel['full_valor']:,.2f}")
            cC.metric("Físico (un)",f"{painel['fisico_unid']:,}".replace(",", "."))
            cD.metric("Físico (R$)",f"R$ {painel['fisico_valor']:,.2f}")

            st.dataframe(df_final, use_container_width=True, height=420)

            with st.expander("Filtros (após geração) — sem recálculo", expanded=False):
                fornecedores = sorted([f for f in df_final["fornecedor"].dropna().astype(str).unique().tolist() if f != ""])
                sel_fornec = st.multiselect("Fornecedor", options=fornecedores, default=[], key=f"filtro_fornec_{empresa}")

                sku_opts = sorted(df_final["SKU"].dropna().astype(str).unique().tolist())
                sel_skus = st.multiselect("SKU (buscar e selecionar)", options=sku_opts, default=[], key=f"filtro_sku_{empresa}")

            df_view = df_final.copy()
            if sel_fornec: df_view = df_view[df_view["fornecedor"].isin(sel_fornec)]
            if sel_skus: df_view = df_view[df_view["SKU"].isin(sel_skus)]

            st.caption(f"Linhas após filtros: {len(df_view)}")
            st.dataframe(df_view, use_container_width=True, height=420)

            colx1, colx2, colx3 = st.columns([1,1,1])
            with colx1:
                if st.button("Baixar XLSX (completo)", key=f"x_all_{empresa}"):
                    xlsx = exportar_xlsx(df_final, h=h, params={"g":g,"LT":LT,"empresa":empresa})
                    st.download_button(
                        "Baixar XLSX (completo)", data=xlsx,
                        file_name=f"Compra_Sugerida_{empresa}_{h}d.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key=f"d_all_{empresa}"
                    )
            with colx2:
                if st.button("Baixar XLSX (filtrado)", key=f"x_fil_{empresa}"):
                    xlsx_filtrado = exportar_xlsx(df_view, h=h, params={"g":g,"LT":LT,"empresa":empresa,"filtro":"on"})
                    st.download_button(
                        "Baixar XLSX (filtrado)", data=xlsx_filtrado,
                        file_name=f"Compra_Sugerida_{empresa}_{h}d_filtrado.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key=f"d_fil_{empresa}"
                    )
            with colx3:
                if st.button("Limpar resultado", key=f"clr_res_{empresa}"):
                    st.session_state["resultado_compra"].pop(empresa, None)
                    st.info("Resultado limpo. Gere novamente se quiser.")

        else:
            st.info("Clique **Gerar Compra** para calcular e então aplicar filtros.")

# ------------------------------ TAB 3 --------------------------------------
with tab3:
    st.subheader("Distribuir quantidade entre empresas — proporcional")
    st.caption("Funciona como antes. (Sem alterações nesta versão.)")

st.caption(f"© Alivvia — {VERSION}")
