# reposicao_facil.py
# Reposição Logística — Alivvia (Streamlit)
# - NÃO acessa Google Sheets automaticamente; só quando você clica (confiável).
# - Lê o XLSX inteiro do Sheets (não depende de gid) e auto-detecta abas KITS/CAT.
# - Mantém a lógica de compra original (NÃO alterada).
# - NOVO:
#     * Abas: 📂 Dados das Empresas | 🛒 Compra Automática | 📦 Alocação de Compra
#     * Salvar uploads fixos (FULL + Shopee) por empresa (até você limpar).
#     * Alocação de Compra proporcional às vendas (FULL + Shopee), sem estoque.
#     * Validações e mensagens claras para evitar erros silenciosos.

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

st.set_page_config(page_title="Reposição Logística — Alivvia", layout="wide")

# ====== SUA PLANILHA FIXA ======
DEFAULT_SHEET_LINK = (
    "https://docs.google.com/spreadsheets/d/1cTLARjq-B5g50dL6tcntg7lb_Iu0ta43/"
    "edit?usp=sharing&ouid=109458533144345974874&rtpof=true&sd=true"
)
DEFAULT_SHEET_ID = "1cTLARjq-B5g50dL6tcntg7lb_Iu0ta43"

# ====== Estado ======
st.session_state.setdefault("catalogo_df", None)   # df: component_sku | fornecedor | status_reposicao
st.session_state.setdefault("kits_df", None)       # df: kit_sku | component_sku | qty
st.session_state.setdefault("loaded_at", None)
st.session_state.setdefault("alt_sheet_link", DEFAULT_SHEET_LINK)

# Salva uploads fixos por empresa (FULL + VENDAS). Estrutura:
# st.session_state.empresa_data = {
#   "ALIVVIA": {"full": DataFrame | None, "vendas": DataFrame | None},
#   "JCA":     {"full": DataFrame | None, "vendas": DataFrame | None},
# }
if "empresa_data" not in st.session_state:
    st.session_state.empresa_data = {"ALIVVIA": {"full": None, "vendas": None},
                                     "JCA": {"full": None, "vendas": None}}

# ============== HTTP util ==============
def _requests_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(total=3, backoff_factor=0.6,
                    status_forcelist=[429, 500, 502, 503, 504],
                    allowed_methods=["GET"])
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
    try:
        sess = _requests_session()
        r = sess.get(url, timeout=30)
        r.raise_for_status()
    except requests.HTTPError as e:
        status = getattr(e.response, "status_code", None)
        raise RuntimeError(
            f"Falha ao baixar XLSX do Google Sheets (HTTP {status}).\n"
            f"URL: {url}\n"
            f"• Compartilhe como 'Qualquer pessoa com o link – Leitor'\n"
            f"• Teste o link em aba anônima\n"
        ) from e
    except Exception as e:
        raise RuntimeError(f"Erro de rede ao baixar XLSX: {url} | {e}") from e
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
    sheet_id = extract_sheet_id_from_url(url)
    if not sheet_id:
        raise RuntimeError("Link inválido do Google Sheets (não encontrei /d/<ID>/).")
    r = sess.get(gs_export_xlsx_url(sheet_id), timeout=30); r.raise_for_status()
    return r.content

# ============== Utils dados ==============
def norm_header(s: str) -> str:
    s = (s or "").strip()
    s = unidecode(s).lower()
    for ch in [" ", "-", "(", ")", "/", "\\", "[", "]", ".", ",", ";", ":"]:
        s = s.replace(ch, "_")
    while "__" in s: s = s.replace("__", "_")
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

# ============== Carregar Padrão do XLSX ==============
def _carregar_padrao_de_content(content: bytes) -> Catalogo:
    try:
        xls = pd.ExcelFile(io.BytesIO(content))
    except Exception as e:
        raise RuntimeError(f"Arquivo XLSX inválido vindo do Google Sheets: {e}")

    def load_sheet(opts):
        for n in opts:
            if n in xls.sheet_names:
                return pd.read_excel(xls, n, dtype=str, keep_default_na=False)
        raise RuntimeError(f"Aba não encontrada. Esperado uma de {opts}. Abas: {xls.sheet_names}")

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
                rename_k[c] = alvo; break
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

# ============== Upload genérico ==============
def load_any_table(uploaded_file) -> Optional[pd.DataFrame]:
    if uploaded_file is None: return None
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
    # FULL com header na 3ª linha (casos com 2 linhas de título)
    if ("sku" not in df.columns) and ("codigo" not in df.columns) and ("codigo_sku" not in df.columns) and len(df) > 0:
        try:
            uploaded_file.seek(0)
            df = pd.read_excel(uploaded_file, dtype=str, keep_default_na=False, header=2)
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

# ============== Detecção & Mapeamento ==============
def mapear_tipo(df: pd.DataFrame) -> str:
    cols = [c.lower() for c in df.columns]
    tem_sku_std  = any(c in {"sku","codigo","codigo_sku"} for c in cols) or any("sku" in c for c in cols)
    tem_vendas60 = any(c.startswith("vendas_60d") or c in {"vendas 60d","vendas_qtd_60d"} for c in cols)
    tem_qtd_livre = any(("qtde" in c) or ("quant" in c) or ("venda" in c) or ("order" in c) for c in cols)
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

# ============== Explosão & Cálculo (original preservado) ==============
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
        "SKU","fornecedor",
        "Vendas_h_ML","Vendas_h_Shopee",
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

# ============== Export XLSX (auditoria forte) ==============
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

# ===================== UI =====================
st.title("Reposição Logística — Alivvia")

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
                cat = carregar_padrao_do_xlsx(DEFAULT_SHEET_ID)
                st.session_state.catalogo_df = cat.catalogo_simples.rename(columns={"component_sku":"sku"})
                st.session_state.kits_df = cat.kits_reais
                st.session_state.loaded_at = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                st.success("Padrão carregado com sucesso.")
            except Exception as e:
                st.session_state.catalogo_df = None
                st.session_state.kits_df = None
                st.session_state.loaded_at = None
                st.error(str(e))
    with colB:
        st.link_button("🔗 Abrir no Drive (editar)", DEFAULT_SHEET_LINK, use_container_width=True)

    st.text_input("Link alternativo do Google Sheets (opcional)", key="alt_sheet_link")
    if st.button("Carregar deste link", use_container_width=True):
        try:
            cat = carregar_padrao_do_link(st.session_state.alt_sheet_link.strip())
            st.session_state.catalogo_df = cat.catalogo_simples.rename(columns={"component_sku":"sku"})
            st.session_state.kits_df = cat.kits_reais
            st.session_state.loaded_at = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            st.success("Padrão carregado a partir do link informado.")
        except Exception as e:
            st.session_state.catalogo_df = None
            st.session_state.kits_df = None
            st.session_state.loaded_at = None
            st.error(str(e))

# Aviso se padrão não está carregado
if st.session_state.catalogo_df is None or st.session_state.kits_df is None:
    st.warning("▶ Carregue o **Padrão (KITS/CAT)** no sidebar antes de usar as abas.")

tab_dados, tab_compra, tab_aloc = st.tabs(["📂 Dados das Empresas", "🛒 Compra Automática", "📦 Alocação de Compra"])

# --------- 📂 Dados das Empresas ---------
with tab_dados:
    st.subheader("Uploads fixos por empresa (mantidos até você limpar)")
    st.caption("Salvamos **FULL** e **Shopee/MT** de cada empresa na memória do app. "
               "Isso evita reenvio frequente. Você pode limpar quando quiser.")

    def empresa_uploader(nome: str):
        st.markdown(f"### {nome}")
        c1, c2, c3 = st.columns([1,1,1])
        with c1:
            full_file = st.file_uploader(f"FULL — {nome}", type=["csv","xlsx","xls"], key=f"full_{nome}")
        with c2:
            vend_file = st.file_uploader(f"Shopee/MT — {nome}", type=["csv","xlsx","xls"], key=f"vend_{nome}")
        with c3:
            st.write("")  # espaçamento
            salvar = st.button(f"Salvar {nome}", use_container_width=True, key=f"save_{nome}")
            limpar = st.button(f"Limpar {nome}", use_container_width=True, key=f"clear_{nome}")

        if salvar:
            try:
                if full_file is None or vend_file is None:
                    st.error("Envie FULL **e** Shopee/MT para salvar.")
                else:
                    df_full_raw = load_any_table(full_file); df_v_raw = load_any_table(vend_file)
                    t1 = mapear_tipo(df_full_raw); t2 = mapear_tipo(df_v_raw)
                    if t1 != "FULL":
                        st.error("Arquivo FULL inválido para salvar."); return
                    if t2 != "VENDAS":
                        st.error("Arquivo Shopee/MT inválido para salvar."); return
                    full_df   = mapear_colunas(df_full_raw, "FULL")
                    vendas_df = mapear_colunas(df_v_raw, "VENDAS")
                    st.session_state.empresa_data[nome]["full"] = full_df
                    st.session_state.empresa_data[nome]["vendas"] = vendas_df
                    st.success(f"Dados de {nome} salvos.")
            except Exception as e:
                st.error(f"Falha ao salvar {nome}: {e}")

        if limpar:
            st.session_state.empresa_data[nome] = {"full": None, "vendas": None}
            st.info(f"Dados de {nome} limpos.")

        # Status
        stat = st.session_state.empresa_data[nome]
        ok_full = stat["full"] is not None
        ok_ven  = stat["vendas"] is not None
        st.caption(f"Status {nome}: FULL [{'OK' if ok_full else '—'}] • Shopee [{'OK' if ok_ven else '—'}]")

    empresa_uploader("ALIVVIA")
    st.markdown("---")
    empresa_uploader("JCA")

# --------- 🛒 Compra Automática ---------
with tab_compra:
    st.subheader("Gerar Compra (por empresa) — lógica original")
    empresa = st.radio("Empresa ativa", ["ALIVVIA", "JCA"], horizontal=True, key="empresa_compra")

    # Uploads da compra (pode usar armazenados)
    c1, c2, c3 = st.columns(3)
    with c1:
        st.markdown("**FULL (Magiic)**")
        full_file = st.file_uploader("CSV/XLSX/XLS", type=["csv","xlsx","xls"], key="comp_full")
    with c2:
        st.markdown("**Estoque Físico**")
        fisico_file = st.file_uploader("CSV/XLSX/XLS", type=["csv","xlsx","xls"], key="comp_fisico")
    with c3:
        st.markdown("**Shopee / Mercado Turbo (vendas por SKU)**")
        vendas_file = st.file_uploader("CSV/XLSX/XLS", type=["csv","xlsx","xls"], key="comp_vendas")

    st.markdown("---")
    if st.button(f"Gerar Compra — {empresa}", type="primary"):
        if st.session_state.catalogo_df is None or st.session_state.kits_df is None:
            st.error("Carregue o **Padrão (KITS/CAT)** no sidebar.")
            st.stop()
        try:
            # Usa upload, senão usa os salvos da empresa (para FULL/VENDAS)
            if full_file is not None:
                full_raw = load_any_table(full_file)
            else:
                full_raw = st.session_state.empresa_data[empresa]["full"]
                if full_raw is None:
                    st.error(f"FULL de {empresa} não enviado nem salvo."); st.stop()
            if vendas_file is not None:
                vendas_raw = load_any_table(vendas_file)
            else:
                vendas_raw = st.session_state.empresa_data[empresa]["vendas"]
                if vendas_raw is None:
                    st.error(f"Shopee/MT de {empresa} não enviado nem salvo."); st.stop()
            if fisico_file is None:
                st.error("Envie o **Estoque Físico** para gerar a compra."); st.stop()
            fisico_raw = load_any_table(fisico_file)

            # Mapeamento (se vieram dos salvos, já estão mapeados)
            if isinstance(full_raw, pd.DataFrame) and "Vendas_Qtd_60d" in full_raw.columns and "Estoque_Full" in full_raw.columns and "Em_Transito" in full_raw.columns:
                full_df = full_raw
            else:
                if mapear_tipo(full_raw) != "FULL": st.error("FULL inválido."); st.stop()
                full_df = mapear_colunas(full_raw, "FULL")

            if isinstance(vendas_raw, pd.DataFrame) and "Quantidade" in vendas_raw.columns and "SKU" in vendas_raw.columns:
                vendas_df = vendas_raw
            else:
                if mapear_tipo(vendas_raw) != "VENDAS": st.error("Shopee/MT inválido."); st.stop()
                vendas_df = mapear_colunas(vendas_raw, "VENDAS")

            if mapear_tipo(fisico_raw) != "FISICO": st.error("Estoque Físico inválido."); st.stop()
            fisico_df = mapear_colunas(fisico_raw, "FISICO")

            cat = Catalogo(
                catalogo_simples=st.session_state.catalogo_df.rename(columns={"sku":"component_sku"}),
                kits_reais=st.session_state.kits_df
            )
            df_final, painel = calcular(full_df, fisico_df, vendas_df, cat, h=h, g=g, LT=LT)
            st.session_state.df_final = df_final
            st.session_state.painel   = painel

            st.success("Cálculo concluído. Use os filtros abaixo sem recálculo.")

            # Painel e export
            df_final_show = st.session_state.df_final.copy()
            painel   = st.session_state.painel

            st.subheader("📊 Painel de Estoques")
            cA, cB, cC, cD = st.columns(4)
            cA.metric("Full (un)",  f"{painel['full_unid']:,}".replace(",", "."))
            cB.metric("Full (R$)",  f"R$ {painel['full_valor']:,.2f}")
            cC.metric("Físico (un)",f"{painel['fisico_unid']:,}".replace(",", "."))
            cD.metric("Físico (R$)",f"R$ {painel['fisico_valor']:,.2f}")

            st.subheader("Itens para comprar")
            st.dataframe(
                df_final_show[df_final_show["Compra_Sugerida"] > 0][[
                    "SKU","fornecedor","Vendas_h_ML","Vendas_h_Shopee",
                    "Estoque_Fisico","Preco","Compra_Sugerida","Valor_Compra_R$"
                ]],
                use_container_width=True
            )

            if st.checkbox("Gerar XLSX (lista + controle)", key="exp_xlsx"):
                try:
                    xlsx_bytes = exportar_xlsx(df_final_show, h=h, params={"g": g, "LT": LT, "empresa": empresa})
                    st.download_button(
                        label=f"Baixar XLSX — Compra_Sugerida_{h}d_{empresa}.xlsx",
                        data=xlsx_bytes,
                        file_name=f"Compra_Sugerida_{h}d_{empresa}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key="btn_dl"
                    )
                except Exception as e:
                    st.error(f"Exportação bloqueada pela Auditoria: {e}")

        except Exception as e:
            st.error(str(e))
            st.stop()

# --------- 📦 Alocação de Compra ---------
with tab_aloc:
    st.subheader("Distribuir quantidade entre empresas — proporcional às vendas (FULL + Shopee)")
    st.caption("Não usa estoque. Usa a soma das vendas do FULL e da Shopee/MT, explodidas por KITS para o componente.")

    if st.session_state.catalogo_df is None or st.session_state.kits_df is None:
        st.error("Carregue o **Padrão (KITS/CAT)** no sidebar."); st.stop()

    # Requer dados salvos de ambas as empresas
    okA = st.session_state.empresa_data["ALIVVIA"]["full"] is not None and st.session_state.empresa_data["ALIVVIA"]["vendas"] is not None
    okJ = st.session_state.empresa_data["JCA"]["full"] is not None and st.session_state.empresa_data["JCA"]["vendas"] is not None
    if not (okA and okJ):
        st.warning("Salve FULL e Shopee/MT para **ALIVVIA** e **JCA** na aba **📂 Dados das Empresas** antes de usar a alocação.")
        st.stop()

    sku_opcoes = st.session_state.catalogo_df["sku"].dropna().astype(str).sort_values().unique().tolist()
    sku_alocar = st.selectbox("SKU do componente para alocar", options=sku_opcoes, index=0, key="aloc_sku_comp")
    qtd_lote = st.number_input("Quantidade total do lote (ex.: 400)", min_value=1, step=1, value=100, key="aloc_qtd_total")

    def demanda60_por_empresa(nome: str, sku_comp: str) -> int:
        try:
            full_df   = st.session_state.empresa_data[nome]["full"]
            vendas_df = st.session_state.empresa_data[nome]["vendas"]
            if full_df is None or vendas_df is None: return 0
            kits = st.session_state.kits_df

            ml_comp = explodir_por_kits(
                full_df[["SKU","Vendas_Qtd_60d"]].rename(columns={"SKU":"kit_sku","Vendas_Qtd_60d":"Qtd"}),
                kits,"kit_sku","Qtd").rename(columns={"Quantidade":"ML_60d"})
            shp_comp = explodir_por_kits(
                vendas_df[["SKU","Quantidade"]].rename(columns={"SKU":"kit_sku","Quantidade":"Qtd"}),
                kits,"kit_sku","Qtd").rename(columns={"Quantidade":"Shopee_60d"})

            sku_comp = norm_sku(sku_comp)
            d = (pd.DataFrame({"SKU": [sku_comp]})
                 .merge(ml_comp, on="SKU", how="left")
                 .merge(shp_comp, on="SKU", how="left"))
            d[["ML_60d","Shopee_60d"]] = d[["ML_60d","Shopee_60d"]].fillna(0).astype(int)
            total_60d = int(max(d.at[0, "ML_60d"] + d.at[0, "Shopee_60d"], d.at[0, "ML_60d"]))
            return total_60d
        except Exception:
            return 0

    if st.button("Calcular alocação proporcional", type="primary", key="btn_calc_aloc"):
        dA = demanda60_por_empresa("ALIVVIA", sku_alocar)
        dJ = demanda60_por_empresa("JCA", sku_alocar)
        soma = dA + dJ

        if soma == 0:
            st.warning("Sem vendas detectadas para esse componente nas duas empresas — alocação 50/50 por falta de base.")
            qA = qtd_lote // 2
            qJ = qtd_lote - qA
            propA = propJ = 0.5
        else:
            propA = dA / soma
            qA = int(round(qtd_lote * propA))
            qJ = int(qtd_lote - qA)
            propJ = 1 - propA

        out = pd.DataFrame([
            {"Empresa": "ALIVVIA", "SKU": sku_alocar, "Demanda_60d": dA, "Proporção": round(propA, 4), "Alocação_Sugerida": qA},
            {"Empresa": "JCA",     "SKU": sku_alocar, "Demanda_60d": dJ, "Proporção": round(propJ, 4), "Alocação_Sugerida": qJ},
        ])
        st.dataframe(out, use_container_width=True)
        st.success(f"Total alocado: {int(out['Alocação_Sugerida'].sum())} un "
                   f"(ALIVVIA {qA} | JCA {qJ})")

        csv = out.to_csv(index=False).encode("utf-8")
        st.download_button("Baixar alocação (.csv)", data=csv,
                           file_name=f"alocacao_{sku_alocar}_{qtd_lote}.csv",
                           mime="text/csv", use_container_width=True)

st.caption("© Alivvia — simples, robusto e auditável.")
