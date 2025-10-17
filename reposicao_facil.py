# reposicao_facil.py
# Reposição Logística — Alivvia (Streamlit)
# - Sem acesso automático ao Google Sheets (só ao clicar).
# - Padrão (KITS/CAT) vem de um XLSX export do Sheets (ID fixo ou link alternativo).
# - Uploads por empresa ficam salvos na memória do app (persistem no reload da página ou navegação).
# - Compra Automática usa a lógica original (FULL + ESTOQUE + VENDAS + KITS/CAT).
# - Alocação de Compra usa apenas FULL + VENDAS (sem estoque), proporcional às vendas explodidas a 60d.
# - Tudo usa os MESMOS arquivos salvos em “Dados das Empresas” (não pede upload de novo).

import io
import re
import hashlib
import datetime as dt
from dataclasses import dataclass
from typing import Optional, Dict, Any

import numpy as np
import pandas as pd
from unidecode import unidecode
import streamlit as st
import requests
from requests.adapters import HTTPAdapter, Retry

# ========================= CONFIG BÁSICA =========================
st.set_page_config(page_title="Reposição Logística — Alivvia", layout="wide")

# Planilha padrão (fixa). Nunca muda? então deixamos embutida aqui.
DEFAULT_SHEET_ID = "1cTLARjq-B5g50dL6tcntg7lb_Iu0ta43"
DEFAULT_SHEET_LINK = (
    "https://docs.google.com/spreadsheets/d/1cTLARjq-B5g50dL6tcntg7lb_Iu0ta43/"
    "edit?usp=sharing&ouid=109458533144345974874&rtpof=true&sd=true"
)

# ========================= ESTADO GLOBAL =========================
# Padrão
st.session_state.setdefault("catalogo_df", None)   # CAT (component_sku/fornecedor/status)
st.session_state.setdefault("kits_df", None)       # KITS (kit_sku/component_sku/qty)
st.session_state.setdefault("padrao_loaded_at", None)
st.session_state.setdefault("alt_sheet_link", DEFAULT_SHEET_LINK)

# Uploads persistentes por empresa (salvos como bytes + nome)
def _empty_slot():
    return {"full": None, "shopee": None, "estoque": None, "meta": {}}

st.session_state.setdefault("EMP_ALIVVIA", _empty_slot())
st.session_state.setdefault("EMP_JCA", _empty_slot())

# ========================= HTTP / SHEETS =========================
def _requests_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=3, backoff_factor=0.6,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/125.0 Safari/537.36"
    })
    return s

def gs_export_xlsx_url(sheet_id: str) -> str:
    return f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=xlsx"

def baixar_xlsx_do_sheets(sheet_id: str) -> bytes:
    url = gs_export_xlsx_url(sheet_id)
    sess = _requests_session()
    try:
        r = sess.get(url, timeout=30)
        r.raise_for_status()
    except requests.HTTPError as e:
        code = getattr(e.response, "status_code", None)
        raise RuntimeError(
            f"Falha ao baixar XLSX do Google Sheets (HTTP {code}).\n"
            f"URL: {url}\n"
            f"• Compartilhe a planilha como 'Qualquer pessoa com o link – Leitor'\n"
            f"• Teste essa URL em janela anônima\n"
        ) from e
    return r.content

def extract_sheet_id_from_url(url: str) -> Optional[str]:
    if not url:
        return None
    m = re.search(r"/d/([a-zA-Z0-9\-_]+)/", url)
    return m.group(1) if m else None

def baixar_xlsx_por_link_google(url: str) -> bytes:
    sess = _requests_session()
    if "export?format=xlsx" in url:
        r = sess.get(url, timeout=30); r.raise_for_status(); return r.content
    sid = extract_sheet_id_from_url(url)
    if not sid:
        raise RuntimeError("Link inválido do Google Sheets (não encontrei /d/<ID>/).")
    r = sess.get(gs_export_xlsx_url(sid), timeout=30); r.raise_for_status(); return r.content

# ========================= UTILS DE DADOS =========================
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
    if isinstance(x, (int, float, np.integer, np.floating)): return float(x)
    s = str(x).strip()
    if s == "": return np.nan
    s = s.replace("\u00a0", " ").replace("R$", "").replace(" ", "").replace(".", "").replace(",", ".")
    try: return float(s)
    except Exception: return np.nan

def norm_sku(x: str) -> str:
    if pd.isna(x): return ""
    return unidecode(str(x)).strip().upper()

def exige_colunas(df: pd.DataFrame, obrig: list, nome_tabela: str):
    faltam = [c for c in obrig if c not in df.columns]
    if faltam:
        raise ValueError(f"Colunas obrigatórias ausentes em {nome_tabela}: {faltam}\nColunas lidas: {list(df.columns)}")

@dataclass
class Catalogo:
    catalogo_simples: pd.DataFrame  # component_sku, fornecedor, status_reposicao
    kits_reais: pd.DataFrame        # kit_sku, component_sku, qty

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
    kits = kits.drop_duplicates(subset=["kit_sku", "component_sku"], keep="first")
    return kits

# ========================= PADRÃO (KITS/CAT) =========================
def _carregar_padrao_de_content(content: bytes) -> Catalogo:
    try:
        xls = pd.ExcelFile(io.BytesIO(content))
    except Exception as e:
        raise RuntimeError(f"Arquivo XLSX inválido vindo do Google Sheets: {e}")

    def load_sheet(opts):
        for n in opts:
            if n in xls.sheet_names:
                return pd.read_excel(xls, n, dtype=str, keep_default_na=False)
        raise RuntimeError(f"Aba não encontrada. Esperado uma de {opts}. Abas existentes: {xls.sheet_names}")

    df_kits = load_sheet(["KITS", "KITS_REAIS", "kits", "kits_reais"]).copy()
    df_cat  = load_sheet(["CATALOGO_SIMPLES", "CATALOGO", "catalogo_simples", "catalogo"]).copy()

    # KITS
    df_kits = normalize_cols(df_kits)
    possiveis_kits = {
        "kit_sku": ["kit_sku", "kit", "sku_kit"],
        "component_sku": ["component_sku", "componente", "sku_componente", "component", "sku_component"],
        "qty": ["qty", "qty_por_kit", "qtd_por_kit", "quantidade_por_kit", "qtd", "quantidade"]
    }
    rename_k = {}
    for alvo, candidatas in possiveis_kits.items():
        for c in candidatas:
            if c in df_kits.columns:
                rename_k[c] = alvo
                break
    df_kits = df_kits.rename(columns=rename_k)
    exige_colunas(df_kits, ["kit_sku", "component_sku", "qty"], "KITS")
    df_kits = df_kits[["kit_sku", "component_sku", "qty"]].copy()
    df_kits["kit_sku"] = df_kits["kit_sku"].map(norm_sku)
    df_kits["component_sku"] = df_kits["component_sku"].map(norm_sku)
    df_kits["qty"] = df_kits["qty"].map(br_to_float).fillna(0).astype(int)
    df_kits = df_kits[df_kits["qty"] >= 1].drop_duplicates(subset=["kit_sku", "component_sku"], keep="first")

    # CATALOGO
    df_cat = normalize_cols(df_cat)
    possiveis_cat = {
        "component_sku": ["component_sku", "sku", "produto", "item", "codigo", "sku_componente"],
        "fornecedor": ["fornecedor", "supplier", "fab", "marca"],
        "status_reposicao": ["status_reposicao", "status", "reposicao_status"]
    }
    rename_c = {}
    for alvo, candidatas in possiveis_cat.items():
        for c in candidatas:
            if c in df_cat.columns:
                rename_c[c] = alvo
                break
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

def carregar_padrao_por_id(sheet_id: str) -> Catalogo:
    content = baixar_xlsx_do_sheets(sheet_id)
    return _carregar_padrao_de_content(content)

def carregar_padrao_por_link(url: str) -> Catalogo:
    content = baixar_xlsx_por_link_google(url)
    return _carregar_padrao_de_content(content)

# ========================= ARQUIVOS (UPLOAD/LEITURA) =========================
def load_any_table_from_bytes(name: str, data: bytes) -> pd.DataFrame:
    bio = io.BytesIO(data)
    name = (name or "").lower()
    if name.endswith(".csv"):
        df = pd.read_csv(bio, dtype=str, keep_default_na=False, sep=None, engine="python")
    else:
        df = pd.read_excel(bio, dtype=str, keep_default_na=False)
    df.columns = [norm_header(c) for c in df.columns]

    # FULL com header na terceira linha? tenta de novo
    if ("sku" not in df.columns) and ("codigo" not in df.columns) and ("codigo_sku" not in df.columns) and len(df) > 0:
        try:
            bio.seek(0)
            df = pd.read_excel(bio, dtype=str, keep_default_na=False, header=2)
            df.columns = [norm_header(c) for c in df.columns]
        except Exception:
            pass

    # limpar "TOTAL/ TOTAIS" (linhas de somatório)
    for c in list(df.columns):
        df = df[~df[c].astype(str).str.contains(r"^TOTALS?$|^TOTAIS?$", case=False, na=False)]

    # normaliza SKU se existir
    cols = set(df.columns)
    sku_col = next((c for c in ["sku","codigo","codigo_sku"] if c in cols), None)
    if sku_col:
        df[sku_col] = df[sku_col].map(norm_sku)
        df = df[df[sku_col] != ""]
    return df.reset_index(drop=True)

def mapear_tipo(df: pd.DataFrame) -> str:
    cols = [c.lower() for c in df.columns]
    tem_sku_std  = any(c in {"sku","codigo","codigo_sku"} for c in cols) or any("sku" in c for c in cols)

    # FULL
    tem_vendas60 = any(c.startswith("vendas_60d") or c in {"vendas 60d","vendas_qtd_60d"} for c in cols)
    tem_estoque_full_like = any(("estoque" in c and "full" in c) or c=="estoque_full" for c in cols)
    tem_transito_like = any(("transito" in c) or c in {"em_transito","em transito","em_transito_full","em_transito_do_anuncio"} for c in cols)
    if tem_sku_std and (tem_vendas60 or tem_estoque_full_like or tem_transito_like):
        return "FULL"

    # FÍSICO
    tem_preco = any(c in {"preco","preco_compra","preco_medio","custo","custo_medio","preco_unitario"} for c in cols)
    tem_estoque_generico  = any(c in {"estoque_atual","qtd","quantidade"} or "estoque" in c for c in cols)
    if tem_sku_std and tem_estoque_generico and tem_preco:
        return "FISICO"

    # VENDAS (Shopee/MT) — robusto pra vários cabeçalhos
    venda_cands = ("qtde","quant","venda","order","orders","qtd","amount","sold","sales","quantity")
    tem_qtd_livre = any(any(tok in c for tok in venda_cands) for c in cols)
    if tem_sku_std and tem_qtd_livre and not tem_preco:
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
            raise RuntimeError("FULL inválido: precisa de coluna SKU/codigo.")

        c_v = [c for c in df.columns if c in ["vendas_qtd_60d","vendas_60d","vendas 60d"] or c.startswith("vendas_60d")]
        if not c_v:
            raise RuntimeError("FULL inválido: faltou Vendas_60d.")
        df["Vendas_Qtd_60d"] = df[c_v[0]].map(br_to_float).fillna(0).astype(int)

        c_e = [c for c in df.columns if c in ["estoque_full","estoque_atual"] or ("estoque" in c and "full" in c)]
        if not c_e:
            raise RuntimeError("FULL inválido: faltou Estoque_Full/estoque_atual.")
        df["Estoque_Full"] = df[c_e[0]].map(br_to_float).fillna(0).astype(int)

        c_t = [c for c in df.columns if c in ["em_transito","em transito","em_transito_full","em_transito_do_anuncio"] or ("transito" in c)]
        df["Em_Transito"] = df[c_t[0]].map(br_to_float).fillna(0).astype(int) if c_t else 0

        return df[["SKU","Vendas_Qtd_60d","Estoque_Full","Em_Transito"]].copy()

    if tipo == "FISICO":
        if "sku" in df.columns:
            sku_series = df["sku"]
        elif "codigo" in df.columns:
            sku_series = df["codigo"]
        elif "codigo_sku" in df.columns:
            sku_series = df["codigo_sku"]
        else:
            cand = next((c for c in df.columns if "sku" in c.lower()), None)
            if cand is None:
                raise RuntimeError("FÍSICO inválido: não achei coluna de SKU.")
            sku_series = df[cand]
        df["SKU"] = sku_series.map(norm_sku)

        c_q = [c for c in df.columns if c in ["estoque_atual","qtd","quantidade"] or ("estoque" in c)]
        if not c_q:
            raise RuntimeError("FÍSICO inválido: faltou Estoque.")
        df["Estoque_Fisico"] = df[c_q[0]].map(br_to_float).fillna(0).astype(int)

        c_p = [c for c in df.columns if c in ["preco","preco_compra","custo","custo_medio","preco_medio","preco_unitario"]]
        if not c_p:
            raise RuntimeError("FÍSICO inválido: faltou Preço/Custo.")
        df["Preco"] = df[c_p[0]].map(br_to_float).fillna(0.0)

        return df[["SKU","Estoque_Fisico","Preco"]].copy()

    if tipo == "VENDAS":
        sku_col = next((c for c in df.columns if "sku" in c.lower()), None)
        if sku_col is None:
            raise RuntimeError("VENDAS inválido: não achei coluna de SKU.")
        df["SKU"] = df[sku_col].map(norm_sku)

        # escolhe coluna de quantidade com score
        cand_qty: list[tuple[int, str]] = []
        for c in df.columns:
            cl = c.lower()
            score = 0
            if any(tok in cl for tok in ("qtde","qtd","quant","quantity")): score += 3
            if any(tok in cl for tok in ("venda","sales","sold")): score += 2
            if any(tok in cl for tok in ("order","orders","amount")): score += 1
            if score > 0:
                cand_qty.append((score, c))
        if not cand_qty:
            raise RuntimeError("VENDAS inválido: não achei coluna de Quantidade.")
        cand_qty.sort(reverse=True)
        qcol = cand_qty[0][1]
        df["Quantidade"] = df[qcol].map(br_to_float).fillna(0).astype(int)
        return df[["SKU","Quantidade"]].copy()

    raise RuntimeError("Tipo de arquivo desconhecido.")

# ========================= LÓGICAS DE CÁLCULO =========================
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

def calcular_compra(full_df, fisico_df, vendas_df, cat: Catalogo, h=60, g=0.0, LT=0):
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

    mask_nao = base["status_reposicao"].str.lower().str.contains("nao_repor", na=False)
    base.loc[mask_nao, "Compra_Sugerida"] = 0

    base["Valor_Compra_R$"] = (base["Compra_Sugerida"].astype(float) * base["Preco"].astype(float)).round(2)
    base["Vendas_h_ML"]     = np.round(base["ML_60d"] * (h/60.0)).astype(int)
    base["Vendas_h_Shopee"] = np.round(base["Shopee_60d"] * (h/60.0)).astype(int)

    base = base.sort_values(["fornecedor","Valor_Compra_R$","SKU"], ascending=[True, False, True])
    df_final = base[[
        "SKU","fornecedor","Vendas_h_ML","Vendas_h_Shopee",
        "Estoque_Fisico","Preco","Compra_Sugerida","Valor_Compra_R$",
        "ML_60d","Shopee_60d","TOTAL_60d","Reserva_30d","Folga_Fisico","Necessidade"
    ]].reset_index(drop=True)

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

def explodir_vendas_para_componente(full_df, vendas_df, cat: Catalogo) -> pd.DataFrame:
    """Soma as vendas de FULL (Vendas_60d) + Shopee (Quantidade) explodidas para COMPONENTE."""
    kits = construir_kits_efetivo(cat)

    full = mapear_colunas(full_df, "FULL")
    shp  = mapear_colunas(vendas_df, "VENDAS")

    ml_comp = explodir_por_kits(
        full[["SKU","Vendas_Qtd_60d"]].rename(columns={"SKU":"kit_sku","Vendas_Qtd_60d":"Qtd"}),
        kits, "kit_sku", "Qtd"
    ).rename(columns={"Quantidade":"ML_60d"})

    shopee_comp = explodir_por_kits(
        shp[["SKU","Quantidade"]].rename(columns={"SKU":"kit_sku","Quantidade":"Qtd"}),
        kits, "kit_sku", "Qtd"
    ).rename(columns={"Quantidade":"Shopee_60d"})

    base = ml_comp.merge(shopee_comp, on="SKU", how="outer")
    base[["ML_60d","Shopee_60d"]] = base[["ML_60d","Shopee_60d"]].fillna(0).astype(int)
    base["Demanda_60d"] = (base["ML_60d"] + base["Shopee_60d"]).astype(int)
    return base[["SKU","Demanda_60d"]]

# ========================= EXPORT XLSX =========================
def sha256_of_csv(df: pd.DataFrame) -> str:
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    return hashlib.sha256(csv_bytes).hexdigest()

def exportar_xlsx(df_final: pd.DataFrame, h: int, params: Dict[str, Any], pendencias: list | None = None) -> bytes:
    int_cols = ["Vendas_h_ML","Vendas_h_Shopee","Estoque_Fisico","Compra_Sugerida","Reserva_30d","Folga_Fisico","Necessidade","ML_60d","Shopee_60d","TOTAL_60d"]
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
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        lista = df_final[df_final["Compra_Sugerida"] > 0].copy()
        lista.to_excel(writer, sheet_name="Lista_Final", index=False)
        ws = writer.sheets["Lista_Final"]
        for i, col in enumerate(lista.columns):
            width = max(12, int(lista[col].astype(str).map(len).max()) + 2)
            ws.set_column(i, i, min(width, 40))
        ws.freeze_panes(1, 0); ws.autofilter(0, 0, len(lista), len(lista.columns)-1)

        if pendencias:
            pd.DataFrame(pendencias).to_excel(writer, sheet_name="Pendencias", index=False)

        ctrl = pd.DataFrame([{
            "data_hora": dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "h": h,
            "linhas_Lista_Final": int((df_final["Compra_Sugerida"] > 0).sum()),
            "soma_Compra_Sugerida": int(df_final["Compra_Sugerida"].sum()),
            "soma_Valor_Compra_R$": float(df_final["Valor_Compra_R$"].sum()),
            "hash_sha256": hash_str,
        } | params])
        ctrl.to_excel(writer, sheet_name="Controle", index=False)
    output.seek(0)
    return output.read()

# ========================= UI — SIDEBAR =========================
with st.sidebar:
    st.subheader("Parâmetros")
    h  = st.selectbox("Horizonte (dias)", [30, 60, 90], index=1)
    g  = st.number_input("Crescimento % ao mês", value=0.0, step=1.0)
    LT = st.number_input("Lead time (dias)", value=0, step=1, min_value=0)

    st.markdown("---")
    st.subheader("Padrão (KITS/CAT) — Google Sheets")
    st.caption("A planilha é fixa; se trocar, altere o ID abaixo ou cole um link alternativo. O app só baixa quando você clicar.")

    colA, colB = st.columns([1,1])
    with colA:
        if st.button("Carregar padrão (ID fixo)", use_container_width=True):
            try:
                cat = carregar_padrao_por_id(DEFAULT_SHEET_ID)
                st.session_state.catalogo_df = cat.catalogo_simples.rename(columns={"component_sku":"sku"})
                st.session_state.kits_df = cat.kits_reais
                st.session_state.padrao_loaded_at = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                st.success("Padrão carregado com sucesso (ID fixo).")
            except Exception as e:
                st.session_state.catalogo_df = None
                st.session_state.kits_df = None
                st.session_state.padrao_loaded_at = None
                st.error(str(e))
    with colB:
        st.link_button("🔗 Abrir no Drive (editar)", DEFAULT_SHEET_LINK, use_container_width=True)

    st.text_input("Link alternativo do Google Sheets (opcional)", key="alt_sheet_link")
    if st.button("Carregar padrão (link)", use_container_width=True):
        try:
            cat = carregar_padrao_por_link(st.session_state.alt_sheet_link.strip())
            st.session_state.catalogo_df = cat.catalogo_simples.rename(columns={"component_sku":"sku"})
            st.session_state.kits_df = cat.kits_reais
            st.session_state.padrao_loaded_at = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            st.success("Padrão carregado com sucesso (link).")
        except Exception as e:
            st.session_state.catalogo_df = None
            st.session_state.kits_df = None
            st.session_state.padrao_loaded_at = None
            st.error(str(e))

# ========================= UI — TABS =========================
st.title("Reposição Logística — Alivvia")
tabs = st.tabs(["📂 Dados das Empresas", "🧮 Compra Automática", "🎯 Alocação de Compra"])

# ---------- Helpers UI ----------
def _save_upload_to_state(slot_name: str, kind: str, upl):
    if upl is None:
        st.warning("Envie um arquivo antes de salvar.")
        return
    content = upl.read()
    st.session_state[slot_name][kind] = {"name": upl.name, "bytes": content}
    st.session_state[slot_name]["meta"][f"{kind}_ts"] = dt.datetime.now().strftime("%Y-%m-%d %H:%M")
    st.success(f"{kind.upper()} salvo para {slot_name.replace('EMP_','')} ({upl.name}).")

def _clear_slot(slot_name: str):
    st.session_state[slot_name] = _empty_slot()
    st.success(f"{slot_name.replace('EMP_','')} limpo.")

def _render_company_block(title: str, slot_name: str):
    st.subheader(title)

    c1, c2 = st.columns(2)
    with c1:
        st.caption("FULL — CSV/XLSX/XLS")
        up_full = st.file_uploader(" ", type=["csv","xlsx","xls"], key=f"up_full_{slot_name}")
        saved = st.session_state[slot_name]["full"]
        if saved:
            st.caption(f"• FULL salvo: {saved['name']} ({st.session_state[slot_name]['meta'].get('full_ts','')})")
    with c2:
        st.caption("Shopee/MT — CSV/XLSX/XLS")
        up_shp = st.file_uploader("  ", type=["csv","xlsx","xls"], key=f"up_shp_{slot_name}")
        saved = st.session_state[slot_name]["shopee"]
        if saved:
            st.caption(f"• Shopee salvo: {saved['name']} ({st.session_state[slot_name]['meta'].get('shopee_ts','')})")

    st.caption("Estoque Físico — opcional (somente para Compra Automática)")
    up_est = st.file_uploader("   ", type=["csv","xlsx","xls"], key=f"up_est_{slot_name}")
    saved = st.session_state[slot_name]["estoque"]
    if saved:
        st.caption(f"• Estoque salvo: {saved['name']} ({st.session_state[slot_name]['meta'].get('estoque_ts','')})")

    colB1, colB2, colB3 = st.columns([1,1,1])
    with colB1:
        if st.button(f"Salvar {title}", key=f"save_{slot_name}"):
            if up_full is None and up_shp is None and up_est is None:
                st.warning("Envie pelo menos 1 arquivo para salvar.")
            else:
                if up_full is not None: _save_upload_to_state(slot_name, "full", up_full)
                if up_shp  is not None: _save_upload_to_state(slot_name, "shopee", up_shp)
                if up_est  is not None: _save_upload_to_state(slot_name, "estoque", up_est)
    with colB2:
        if st.button(f"Limpar {title}", key=f"clear_{slot_name}"):
            _clear_slot(slot_name)

def _load_saved_df(slot_name: str, kind: str) -> Optional[pd.DataFrame]:
    blob = st.session_state.get(slot_name, {}).get(kind)
    if not blob:
        return None
    return load_any_table_from_bytes(blob["name"], blob["bytes"])

def _need_padroes():
    if st.session_state.catalogo_df is None or st.session_state.kits_df is None:
        st.warning("Carregue o **Padrão (KITS/CAT)** no sidebar antes de usar.")
        return True
    return False

# ========================= TAB 1: DADOS =========================
with tabs[0]:
    st.markdown("Uploads fixos por empresa (mantidos até você limpar).")
    _render_company_block("ALIVVIA", "EMP_ALIVVIA")
    st.divider()
    _render_company_block("JCA", "EMP_JCA")
    st.caption("© Alivvia — simples, robusto e auditável.")

# ========================= TAB 2: COMPRA AUTOMÁTICA =========================
with tabs[1]:
    st.header("Gerar Compra (por empresa) — lógica original")

    if _need_padroes():
        st.stop()

    empresa = st.radio("Empresa ativa", ["ALIVVIA", "JCA"], horizontal=True, key="buy_emp")
    slot = "EMP_ALIVVIA" if empresa == "ALIVVIA" else "EMP_JCA"

    # Mostra chips do que está salvo
    ch1, ch2, ch3 = st.columns(3)
    with ch1:
        if st.session_state[slot]["full"]:
            st.info(f"FULL: {st.session_state[slot]['full']['name']}")
        else:
            st.error("FULL não salvo.")
    with ch2:
        if st.session_state[slot]["shopee"]:
            st.info(f"Shopee/MT: {st.session_state[slot]['shopee']['name']}")
        else:
            st.error("Shopee/MT não salvo.")
    with ch3:
        if st.session_state[slot]["estoque"]:
            st.info(f"Estoque: {st.session_state[slot]['estoque']['name']}")
        else:
            st.warning("Estoque não salvo (obrigatório para compra).")

    if st.button(f"Gerar Compra — {empresa}", type="primary"):
        try:
            full_raw = _load_saved_df(slot, "full")
            shp_raw  = _load_saved_df(slot, "shopee")
            est_raw  = _load_saved_df(slot, "estoque")

            if full_raw is None or shp_raw is None or est_raw is None:
                st.error("Para **Compra Automática** é obrigatório ter FULL, Shopee/MT e Estoque salvos.")
                st.stop()

            # Mapeia e valida tipos
            tipos = {}
            for name, df in [("FULL", full_raw), ("VENDAS", shp_raw), ("FISICO", est_raw)]:
                t = mapear_tipo(df)
                if t != name:
                    st.error(f"Arquivo {name} não foi reconhecido como {name}. Detectei: {t}.")
                    st.stop()
                tipos[name] = t

            full_df   = mapear_colunas(full_raw, "FULL")
            vendas_df = mapear_colunas(shp_raw, "VENDAS")
            fisico_df = mapear_colunas(est_raw, "FISICO")

            cat = Catalogo(
                catalogo_simples=st.session_state.catalogo_df.rename(columns={"sku":"component_sku"}),
                kits_reais=st.session_state.kits_df
            )
            df_final, painel = calcular_compra(full_df, fisico_df, vendas_df, cat, h=h, g=g, LT=LT)

            st.success("Cálculo concluído.")
            st.subheader("📊 Painel de Estoques")
            cA, cB, cC, cD = st.columns(4)
            cA.metric("Full (un)",  f"{painel['full_unid']:,}".replace(",", "."))
            cB.metric("Full (R$)",  f"R$ {painel['full_valor']:,.2f}")
            cC.metric("Físico (un)",f"{painel['fisico_unid']:,}".replace(",", "."))
            cD.metric("Físico (R$)",f"R$ {painel['fisico_valor']:,.2f}")

            st.divider()
            st.subheader("Itens para comprar")
            st.dataframe(
                df_final[df_final["Compra_Sugerida"] > 0][[
                    "SKU","fornecedor","Vendas_h_ML","Vendas_h_Shopee",
                    "Estoque_Fisico","Preco","Compra_Sugerida","Valor_Compra_R$"
                ]],
                use_container_width=True, height=420
            )

            if st.checkbox("Gerar XLSX (com auditoria)?", key="chk_export_buy"):
                try:
                    xlsx_bytes = exportar_xlsx(df_final, h=h, params={"g": g, "LT": LT, "empresa": empresa})
                    st.download_button(
                        label=f"Baixar XLSX — Compra_Sugerida_{h}d_{empresa}.xlsx",
                        data=xlsx_bytes,
                        file_name=f"Compra_Sugerida_{h}d_{empresa}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key="btn_dl_buy"
                    )
                except Exception as e:
                    st.error(f"Exportação bloqueada: {e}")

        except Exception as e:
            st.error(str(e))

# ========================= TAB 3: ALOCAÇÃO (somente vendas) =========================
with tabs[2]:
    st.header("Distribuir quantidade entre empresas — proporcional às vendas (FULL + Shopee)")

    if _need_padroes():
        st.stop()

    # precisa apenas de FULL e Shopee salvos em cada empresa
    faltas = []
    for emp, slot in [("ALIVVIA","EMP_ALIVVIA"), ("JCA","EMP_JCA")]:
        if st.session_state[slot]["full"] is None: faltas.append(f"{emp}: FULL")
        if st.session_state[slot]["shopee"] is None: faltas.append(f"{emp}: Shopee")
    if faltas:
        st.error("Para alocação, salve FULL e Shopee de **ambas** as empresas em 'Dados das Empresas'.\n" +
                 "Faltando: " + ", ".join(faltas))
        st.stop()

    # Catálogo de componentes (para autocomplete)
    CATALOGO = st.session_state.catalogo_df.copy()
    skus_componentes = CATALOGO["sku"].dropna().astype(str).sort_values().unique().tolist()

    sku_alocar = st.selectbox("SKU do componente para alocar", options=skus_componentes, index=0)
    quantidade_lote = st.number_input("Quantidade total do lote (ex.: 400)", min_value=1, value=1000, step=10)

    if st.button("Calcular alocação proporcional", type="primary"):
        try:
            # Leitura dos salvos
            full_A = _load_saved_df("EMP_ALIVVIA", "full")
            shp_A  = _load_saved_df("EMP_ALIVVIA", "shopee")
            full_J = _load_saved_df("EMP_JCA", "full")
            shp_J  = _load_saved_df("EMP_JCA", "shopee")

            # Detecta/mapeia para colunas reduzidas esperadas
            def _prep(df: pd.DataFrame, expect: str) -> pd.DataFrame:
                t = mapear_tipo(df)
                if t != expect:
                    raise RuntimeError(f"Arquivo {expect} não reconhecido (detectado {t}).")
                return mapear_colunas(df, expect)

            full_A = _prep(full_A, "FULL"); shp_A = _prep(shp_A, "VENDAS")
            full_J = _prep(full_J, "FULL"); shp_J = _prep(shp_J, "VENDAS")

            cat = Catalogo(
                catalogo_simples=st.session_state.catalogo_df.rename(columns={"sku":"component_sku"}),
                kits_reais=st.session_state.kits_df
            )
            # Explode para componente e soma 60d
            venda_A = explodir_vendas_para_componente(full_A, shp_A, cat)
            venda_J = explodir_vendas_para_componente(full_J, shp_J, cat)

            dA = int(venda_A.loc[venda_A["SKU"] == norm_sku(sku_alocar), "Demanda_60d"].sum())
            dJ = int(venda_J.loc[venda_J["SKU"] == norm_sku(sku_alocar), "Demanda_60d"].sum())

            total = dA + dJ
            if total == 0:
                st.warning("Sem vendas detectadas para esse componente nas duas empresas — alocação 50/50 por falta de base.")
                pA = pJ = 0.5
            else:
                pA = dA / total
                pJ = dJ / total

            aA = int(round(quantidade_lote * pA))
            aJ = int(quantidade_lote) - aA  # garante soma perfeita

            df_aloc = pd.DataFrame([
                {"Empresa":"ALIVVIA","SKU": norm_sku(sku_alocar), "Demanda_60d": dA, "Proporção": round(pA,4), "Alocação_Sugerida": aA},
                {"Empresa":"JCA",    "SKU": norm_sku(sku_alocar), "Demanda_60d": dJ, "Proporção": round(pJ,4), "Alocação_Sugerida": aJ},
            ])
            st.dataframe(df_aloc, use_container_width=True, height=220)
            st.success(f"Total alocado: {aA + aJ} un (ALIVVIA {aA} | JCA {aJ})")

            # Download CSV simples
            csv = df_aloc.to_csv(index=False).encode("utf-8")
            st.download_button("Baixar alocação (.csv)", data=csv, file_name=f"Alocacao_{norm_sku(sku_alocar)}_{dt.date.today()}.csv")

        except Exception as e:
            st.error(f"Falha ao calcular alocação: {e}")

st.caption("© Alivvia — simples, robusto e auditável.")
