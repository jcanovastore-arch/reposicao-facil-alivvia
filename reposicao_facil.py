# reposicao_facil.py
# Reposição Logística — Alivvia (Streamlit)
# - NENHUM acesso automático ao Google Sheets.
# - Só lê o padrão (KITS/CAT) quando você clicar no botão "Carregar padrão agora".
# - Link padrão já preenchido; pode trocar na UI e recarregar.
# - Mantém a lógica original de cálculo/UX.

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

# ==========================
# Config Streamlit (primeira chamada)
# ==========================
st.set_page_config(page_title="Reposição Logística — Alivvia", layout="wide")

# ==========================
# Defaults
# ==========================
DEFAULT_SHEET_LINK = (
    "https://docs.google.com/spreadsheets/d/1cTLARjq-B5g50dL6tcntg7lb_Iu0ta43/"
    "edit?usp=sharing&ouid=109458533144345974874&rtpof=true&sd=true"
)
DEFAULT_GID_KITS = "1589453187"   # confirmei com você
DEFAULT_GID_CAT  = "0"            # ajuste se o catálogo tiver outro gid

# ==========================
# Estado
# ==========================
st.session_state.setdefault("SHEET_LINK", DEFAULT_SHEET_LINK)  # link que você pode trocar na UI
st.session_state.setdefault("SHEET_ID", None)                  # só é preenchido quando clicar no botão
st.session_state.setdefault("GID_KITS", DEFAULT_GID_KITS)
st.session_state.setdefault("GID_CATALOGO", DEFAULT_GID_CAT)

st.session_state.setdefault("catalogo_df", None)  # DataFrame do catálogo carregado
st.session_state.setdefault("kits_df", None)      # DataFrame de kits carregado
st.session_state.setdefault("loaded_at", None)    # timestamp carregamento

st.session_state.setdefault("df_final", None)     # resultado de cálculo
st.session_state.setdefault("painel", None)

# ==========================
# Utils de Sheets/requests
# ==========================
def _is_google_sheets_url(url: str) -> bool:
    return bool(re.match(r"^https://docs\.google\.com/spreadsheets/d/[A-Za-z0-9\-_]+", (url or "").strip()))

def parse_google_sheets_link(url: str) -> Tuple[Optional[str], Optional[str]]:
    """Aceita links de edição (…/edit#gid=...) ou export (…/export?format=csv&gid=...)."""
    if not url or not _is_google_sheets_url(url):
        return None, None
    m = re.search(r"/spreadsheets/d/([A-Za-z0-9\-_]+)", url)
    sheet_id = m.group(1) if m else None
    gid = None
    m_gid = re.search(r"(?:[#?&])gid=(\d+)", url)
    if m_gid:
        gid = m_gid.group(1)
    return sheet_id, gid

def gs_export_csv_url(sheet_id: str, gid: str) -> str:
    return f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"

def _requests_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=3, backoff_factor=0.6,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/125.0 Safari/537.36"
    })
    return s

def read_gs_csv(sheet_id: str, gid: str) -> pd.DataFrame:
    url = gs_export_csv_url(sheet_id, gid)
    try:
        sess = _requests_session()
        r = sess.get(url, timeout=30)
        r.raise_for_status()
    except requests.HTTPError as e:
        status = getattr(e.response, "status_code", None)
        msg = (f"Falha ao baixar CSV do Google Sheets (HTTP {status}).\n"
               f"URL: {url}\n"
               f"Dicas:\n"
               f"• Compartilhar: 'Qualquer pessoa com o link' → Leitor.\n"
               f"• Teste em aba anônima.\n"
               f"• Confirme o gid da aba (abra a aba e veja '#gid=').")
        raise RuntimeError(msg) from e
    except Exception as e:
        raise RuntimeError(f"Erro de rede ao baixar CSV: {url} | {e}") from e

    try:
        return pd.read_csv(io.BytesIO(r.content), dtype=str, keep_default_na=False)
    except Exception as e:
        raise RuntimeError(f"CSV inválido (não deu pra ler): {url} | {e}") from e

# ==========================
# Utils de dados
# ==========================
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
    if s == "":
        return np.nan
    s = s.replace("\u00a0", " ")
    s = s.replace("R$", "").replace(" ", "")
    s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except Exception:
        return np.nan

def norm_sku(x: str) -> str:
    if pd.isna(x):
        return ""
    return unidecode(str(x)).strip().upper()

def exige_colunas(df: pd.DataFrame, obrig: list, nome_tabela: str):
    faltam = [c for c in obrig if c not in df.columns]
    if faltam:
        raise ValueError(
            f"Colunas obrigatórias ausentes em {nome_tabela}: {faltam}\n"
            f"Colunas lidas: {list(df.columns)}"
        )

@dataclass
class Catalogo:
    catalogo_simples: pd.DataFrame  # component_sku, fornecedor, status_reposicao (opcional)
    kits_reais: pd.DataFrame        # kit_sku, component_sku, qty (>=1)

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

# ==========================
# Carregar Padrão (apenas quando clicar no botão)
# ==========================
def carregar_padrao_do_sheets(link: str, gid_kits: str, gid_cat: str) -> Catalogo:
    sheet_id, gid_link = parse_google_sheets_link(link)
    if not sheet_id:
        raise RuntimeError("Link do Google Sheets inválido. Cole a URL completa da planilha/aba.")
    # salva o ID no estado (apenas agora)
    st.session_state.SHEET_ID = sheet_id

    # --- KITS ---
    kits = read_gs_csv(sheet_id, gid_kits)
    kits = normalize_cols(kits)
    possiveis_kits = {
        "kit_sku": ["kit_sku", "kit", "sku_kit"],
        "component_sku": ["component_sku", "componente", "sku_componente", "component", "sku_component"],
        "qty": ["qty", "qty_por_kit", "qtd_por_kit", "quantidade_por_kit", "qtd", "quantidade"]
    }
    rename_k = {}
    for alvo, candidatas in possiveis_kits.items():
        for c in candidatas:
            if c in kits.columns:
                rename_k[c] = alvo
                break
    kits = kits.rename(columns=rename_k)
    exige_colunas(kits, ["kit_sku", "component_sku", "qty"], "KITS")
    kits = kits[["kit_sku", "component_sku", "qty"]].copy()
    kits["kit_sku"] = kits["kit_sku"].map(norm_sku)
    kits["component_sku"] = kits["component_sku"].map(norm_sku)
    kits["qty"] = kits["qty"].map(br_to_float).fillna(0).astype(int)
    kits = kits[kits["qty"] >= 1]
    kits = kits.drop_duplicates(subset=["kit_sku", "component_sku"], keep="first")

    # --- CATALOGO ---
    catalogo = read_gs_csv(sheet_id, gid_cat)
    catalogo = normalize_cols(catalogo)
    possiveis_cat = {
        "component_sku": ["component_sku", "sku", "produto", "item", "codigo", "sku_componente"],
        "fornecedor": ["fornecedor", "supplier", "fab", "marca"],
        "status_reposicao": ["status_reposicao", "status", "reposicao_status"]
    }
    rename_c = {}
    for alvo, candidatas in possiveis_cat.items():
        for c in candidatas:
            if c in catalogo.columns:
                rename_c[c] = alvo
                break
    catalogo = catalogo.rename(columns=rename_c)
    if "component_sku" not in catalogo.columns:
        raise ValueError("CATALOGO precisa ter coluna 'component_sku' (ou 'sku').")
    if "fornecedor" not in catalogo.columns:
        catalogo["fornecedor"] = ""
    if "status_reposicao" not in catalogo.columns:
        catalogo["status_reposicao"] = ""

    catalogo["component_sku"] = catalogo["component_sku"].map(norm_sku)
    catalogo["fornecedor"] = catalogo["fornecedor"].fillna("").astype(str)
    catalogo["status_reposicao"] = catalogo["status_reposicao"].fillna("").astype(str)
    catalogo = catalogo.drop_duplicates(subset=["component_sku"], keep="last")

    return Catalogo(catalogo_simples=catalogo, kits_reais=kits)

# ==========================
# Leitura genérica dos uploads
# ==========================
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
    # FULL com header na 3ª linha
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

# ==========================
# Detecção & Mapeamento
# ==========================
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
            if cand is None:
                raise RuntimeError("FÍSICO inválido: não achei coluna de SKU.")
            sku_series = df[cand]
        df["SKU"] = sku_series.map(norm_sku)

        c_q = [c for c in df.columns if c in ["estoque_atual","qtd","quantidade"] or ("estoque" in c)]
        if not c_q: raise RuntimeError("FÍSICO inválido: faltou Estoque (estoque_atual/qtd/quantidade).")
        df["Estoque_Fisico"] = df[c_q[0]].map(br_to_float).fillna(0).astype(int)

        c_p = [c for c in df.columns if c in ["preco","preco_compra","custo","custo_medio","preco_medio","preco_unitario"]]
        if not c_p: raise RuntimeError("FÍSICO inválido: faltou Preço/Custo.")
        df["Preco"] = df[c_p[0]].map(br_to_float).fillna(0.0)

        return df[["SKU","Estoque_Fisico","Preco"]].copy()

    if tipo == "VENDAS":
        sku_col = next((c for c in df.columns if "sku" in c.lower()), None)
        if sku_col is None:
            raise RuntimeError("VENDAS inválido: não achei coluna de SKU (ex.: SKU, Model SKU, Variation SKU).")
        df["SKU"] = df[sku_col].map(norm_sku)

        cand_qty = []
        for c in df.columns:
            cl = c.lower()
            score = 0
            if "qtde"  in cl: score += 3
            if "quant" in cl: score += 2
            if "venda" in cl: score += 1
            if "order" in cl: score += 1
            if score > 0:
                cand_qty.append((score, c))
        if not cand_qty:
            raise RuntimeError("VENDAS inválido: não achei coluna de Quantidade (ex.: Qtde. Vendas, Quantidade, Orders).")
        cand_qty.sort(reverse=True)
        qcol = cand_qty[0][1]
        df["Quantidade"] = df[qcol].map(br_to_float).fillna(0).astype(int)
        return df[["SKU","Quantidade"]].copy()

    raise RuntimeError("Tipo de arquivo desconhecido.")

# ==========================
# Explosão & Cálculo
# ==========================
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

# ==========================
# Export XLSX
# ==========================
def sha256_of_csv(df: pd.DataFrame) -> str:
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    return hashlib.sha256(csv_bytes).hexdigest()

def exportar_xlsx(df_final: pd.DataFrame, h: int, params: dict, pendencias: list | None = None) -> bytes:
    int_cols = ["Vendas_h_ML","Vendas_h_Shopee","Estoque_Fisico","Compra_Sugerida",
                "Reserva_30d","Folga_Fisico","Necessidade","ML_60d","Shopee_60d","TOTAL_60d"]
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

# ==========================
# UI
# ==========================
st.title("Reposição Logística — Alivvia")
st.caption("Sem acesso automático ao Google Sheets. Clique em **Carregar padrão agora** para ler KITS/CAT.")

with st.sidebar:
    st.subheader("Parâmetros")
    h  = st.selectbox("Horizonte (dias)", [30, 60, 90], index=1)
    g  = st.number_input("Crescimento % ao mês", value=0.0, step=1.0)
    LT = st.number_input("Lead time (dias)", value=0, step=1, min_value=0)

    st.markdown("---")
    st.subheader("Padrão (KITS/CAT) — Google Sheets")
    st.session_state.SHEET_LINK = st.text_input("Link da planilha (edit/export)", value=st.session_state.SHEET_LINK)
    st.session_state.GID_KITS = st.text_input("gid KITS", value=st.session_state.GID_KITS)
    st.session_state.GID_CATALOGO = st.text_input("gid CATALOGO", value=st.session_state.GID_CATALOGO)

    colA, colB = st.columns([1, 1])
    with colA:
        if st.button("Carregar padrão agora", use_container_width=True):
            try:
                cat = carregar_padrao_do_sheets(
                    st.session_state.SHEET_LINK,
                    st.session_state.GID_KITS,
                    st.session_state.GID_CATALOGO
                )
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
        abrir_url = st.session_state.SHEET_LINK if _is_google_sheets_url(st.session_state.SHEET_LINK) else DEFAULT_SHEET_LINK
        st.link_button("🔗 Abrir no Drive (editar)", abrir_url, use_container_width=True)

# Empresa
st.subheader("Empresa")
empresa = st.radio("Empresa ativa", ["ALIVVIA", "JCA"], horizontal=True)

# Status do padrão
if st.session_state.catalogo_df is None or st.session_state.kits_df is None:
    st.info("✅ Link padrão já preenchido.\n\n➡️ Clique em **Carregar padrão agora** na barra lateral para ler KITS/CAT.\n\n"
            "Depois envie os 3 arquivos (FULL, Estoque Físico, Vendas) e gere a compra.")
else:
    st.success(f"Padrão carregado em {st.session_state.loaded_at} • Sheet ID: {st.session_state.SHEET_ID} • "
               f"GIDs: KITS={st.session_state.GID_KITS}, CAT={st.session_state.GID_CATALOGO}")

# Uploads
st.subheader("Uploads")
c1, c2, c3 = st.columns(3)
with c1:
    st.markdown("**FULL (Magiic)**")
    full_file = st.file_uploader("CSV/XLSX/XLS", type=["csv","xlsx","xls"], key="full")
with c2:
    st.markdown("**Estoque Físico**")
    fisico_file = st.file_uploader("CSV/XLSX/XLS", type=["csv","xlsx","xls"], key="fisico")
with c3:
    st.markdown("**Shopee / Mercado Turbo (vendas por SKU)**")
    vendas_file = st.file_uploader("CSV/XLSX/XLS", type=["csv","xlsx","xls"], key="vendas")

# Filtros (só se padrão estiver carregado)
if st.session_state.catalogo_df is not None:
    st.markdown("---")
    st.subheader("Filtros")
    CATALOGO = st.session_state.catalogo_df
    fornecedores_lista = sorted([f for f in CATALOGO["fornecedor"].dropna().unique().tolist() if str(f).strip() != ""])
    fornecedores_sel = st.multiselect("Filtrar por Fornecedor", options=fornecedores_lista)
    sku_opcoes = CATALOGO["sku"].dropna().astype(str).sort_values().unique().tolist()
    sku_sel = st.multiselect("Filtrar por SKU (busca)", options=sku_opcoes)

    with st.expander("📄 Catálogo (após filtros) — referência", expanded=False):
        catalogo_filtrado = CATALOGO.copy()
        if fornecedores_sel:
            catalogo_filtrado = catalogo_filtrado[catalogo_filtrado["fornecedor"].isin(fornecedores_sel)]
        if sku_sel:
            catalogo_filtrado = catalogo_filtrado[catalogo_filtrado["sku"].astype(str).isin(sku_sel)]
        st.dataframe(catalogo_filtrado, use_container_width=True, height=260)

# Botão de cálculo
st.markdown("---")
if st.button(f"Gerar Compra — {empresa}", type="primary"):
    if st.session_state.catalogo_df is None or st.session_state.kits_df is None:
        st.error("Carregue o **Padrão (KITS/CAT)** primeiro (botão no sidebar).")
        st.stop()
    try:
        full_raw   = load_any_table(full_file)   if full_file   is not None else None
        fisico_raw = load_any_table(fisico_file) if fisico_file is not None else None
        vendas_raw = load_any_table(vendas_file) if vendas_file is not None else None

        if full_raw is None or fisico_raw is None or vendas_raw is None:
            st.error("Envie **FULL**, **Estoque Físico** e **Vendas (Shopee/MT)** para gerar a compra.")
            st.stop()

        dfs = []
        for df_up in [full_raw, fisico_raw, vendas_raw]:
            t = mapear_tipo(df_up)
            if t == "DESCONHECIDO":
                st.error("Um dos arquivos não foi reconhecido (FULL/FISICO/VENDAS). Reexporte com colunas corretas.")
                st.stop()
            dfs.append((t, mapear_colunas(df_up, t)))

        full_df   = [df for t, df in dfs if t == "FULL"][0]
        fisico_df = [df for t, df in dfs if t == "FISICO"][0]
        vendas_df = [df for t, df in dfs if t == "VENDAS"][0]

        cat = Catalogo(
            catalogo_simples=st.session_state.catalogo_df.rename(columns={"sku":"component_sku"}),
            kits_reais=st.session_state.kits_df
        )

        df_final, painel = calcular(full_df, fisico_df, vendas_df, cat, h=h, g=g, LT=LT)

        st.session_state.df_final = df_final
        st.session_state.painel   = painel

        st.success("Cálculo concluído. Use os filtros abaixo sem recálculo.")

    except Exception as e:
        st.error(str(e))
        st.stop()

# Pós-cálculo
if st.session_state.df_final is not None:
    df_final = st.session_state.df_final.copy()
    painel   = st.session_state.painel

    st.subheader("📊 Painel de Estoques")
    cA, cB, cC, cD = st.columns(4)
    cA.metric("Full (un)",  f"{painel['full_unid']:,}".replace(",", "."))
    cB.metric("Full (R$)",  f"R$ {painel['full_valor']:,.2f}")
    cC.metric("Físico (un)",f"{painel['fisico_unid']:,}".replace(",", "."))
    cD.metric("Físico (R$)",f"R$ {painel['fisico_valor']:,.2f}")

    st.divider()

    fornecedores_da_compra = sorted(df_final["fornecedor"].fillna("").unique())
    sel_fornec = st.multiselect("Filtrar visão por Fornecedor", fornecedores_da_compra, key="filt_fornec_compra")
    mostra = df_final if not sel_fornec else df_final[df_final["fornecedor"].isin(sel_fornec)]

    if sel_fornec:
        st.info(f"Filtro ativo: {len(sel_fornec)} fornecedor(es) • {mostra['SKU'].nunique()} SKUs na visão.")
    else:
        st.caption("Nenhum filtro por fornecedor aplicado na visão.")

    with st.expander("🔎 Prévia por SKU (opcional)"):
        sku_opts = sorted(mostra["SKU"].unique())
        sel_skus = st.multiselect("Escolha 1 ou mais SKUs", sku_opts, key="filt_sku_preview")
        prev = mostra if not sel_skus else mostra[mostra["SKU"].isin(sel_skus)]
        st.dataframe(
            prev[[
                "SKU","fornecedor","ML_60d","Shopee_60d","TOTAL_60d",
                "Estoque_Fisico","Reserva_30d","Folga_Fisico",
                "Necessidade","Compra_Sugerida","Preco"
            ]],
            use_container_width=True,
            height=380
        )
        st.caption("Compra = Necessidade − Folga (nunca negativa). Vendas 60d já explodidas e Shopee normalizada.")

    st.subheader("Itens para comprar (copiável)")
    st.dataframe(
        mostra[mostra["Compra_Sugerida"] > 0][[
            "SKU","fornecedor","Vendas_h_ML","Vendas_h_Shopee",
            "Estoque_Fisico","Preco","Compra_Sugerida","Valor_Compra_R$"
        ]],
        use_container_width=True
    )

    compra_total = int(mostra["Compra_Sugerida"].sum())
    valor_total  = float(mostra["Valor_Compra_R$"].sum())
    st.success(f"{len(mostra[mostra['Compra_Sugerida']>0])} SKUs com compra > 0 | Compra total: {compra_total} un | Valor: R$ {valor_total:,.2f}")

    st.subheader("Exportação XLSX (Lista_Final + Controle)")
    if st.checkbox("Gerar planilha XLSX com hash e auditoria (sem recálculo)?", key="chk_export"):
        try:
            xlsx_bytes = exportar_xlsx(mostra, h=h, params={"g": g, "LT": LT, "empresa": empresa})
            st.download_button(
                label=f"Baixar XLSX — Compra_Sugerida_{h}d_{empresa}.xlsx",
                data=xlsx_bytes,
                file_name=f"Compra_Sugerida_{h}d_{empresa}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="btn_dl"
            )
            st.info("Planilha gerada a partir do mesmo DataFrame exibido (paridade garantida).")
        except Exception as e:
            st.error(f"Exportação bloqueada pela Auditoria: {e}")

st.caption("© Alivvia — simples, robusto e auditável.")
