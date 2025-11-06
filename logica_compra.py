# logica_compra.py - V11.4 (PATCH HIPER-DEFENSIVO - FINAL CLEAN)
# - FIX: Remove caracteres invisíveis que causaram o erro fatal de sintaxe.
# - Contém a lógica robusta (V11.3) de checagem de abas/colunas/preços em Pandas.

import io
import re
import hashlib
import datetime as dt
from dataclasses import dataclass
from typing import Optional, Tuple, List, Dict, Any

import numpy as np
import pandas as pd
from unidecode import unidecode
import requests
from requests.adapters import HTTPAdapter, Retry

# ===================== CONFIG BÁSICA =====================
DEFAULT_SHEET_ID = "1cTLARjq-B5g50dL6tcntg7lb_Iu0ta43"

# ===================== HTTP / GOOGLE SHEETS =====================
def _requests_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(total=3, backoff_factor=0.6, status_forcelist=[429,500,502,503,504], allowed_methods=["GET"])
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.headers.update({"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit(537.36, like Gecko) Chrome/125 Safari/537.36"})
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
_BR_MONEY_RE = re.compile(r"[^\d,.-]+")

def br_to_float(series_or_scalar):
    """Converte valores BR ('R$ 12,90') em float. Aceita escalar ou Series (FIX AMBIGUOUS)."""
    if isinstance(series_or_scalar, pd.Series):
        s = series_or_scalar.astype(str).str.replace(".", "", regex=False)
        s = s.str.replace(",", ".", regex=False)
        s = s.str.replace(_BR_MONEY_RE, "", regex=True)
        out = pd.to_numeric(s, errors="coerce").astype(float)
        return out.fillna(0.0)
    else:
        s = str(series_or_scalar).replace(".", "").replace(",", ".")
        s = _BR_MONEY_RE.sub("", s)
        try:
            return float(s)
        except Exception:
            return 0.0

def norm_header(s: str) -> str:
    """Colunas em minúsculas e normalizadas."""
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


# ===================== PADRÃO KITS/CAT (PATCH CLEAN) =====================
@dataclass
class Catalogo:
    catalogo_simples: pd.DataFrame
    kits_reais: pd.DataFrame

def _to_lc_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Colunas em minúsculas, sem espaços extras."""
    m = {c: c.strip().lower() for c in df.columns}
    return df.rename(columns=m)

def _pick_sheet_ci(xls: pd.ExcelFile, *candidates) -> pd.DataFrame:
    """Escolhe aba por substring case-insensitive. (Sua lógica limpa)"""
    names = {name.lower(): name for name in xls.sheet_names}
    for cand in candidates:
        cand_lc = cand.lower()
        for lc, real in names.items():
            if cand_lc in lc:
                return pd.read_excel(xls, real, dtype=str, keep_default_na=False)
    raise RuntimeError(f"Aba não encontrada: tente nomes contendo {candidates}. Abas lidas: {list(xls.sheet_names)}")

def _normalize_catalogo(df_raw: pd.DataFrame) -> pd.DataFrame:
    """Gera DataFrame de Catálogo padronizado, usando lógica robusta."""
    df = _to_lc_columns(df_raw).copy()

    sku_cols = [c for c in df.columns if "component" in c and "sku" in c] or \
               [c for c in df.columns if c in ("component_sku","sku","codigo_sku","codigo")]
    forn_cols = [c for c in df.columns if c in ("fornecedor","supplier","vendor")]
    status_cols = [c for c in df.columns if "status" in c and "repos" in c] or \
                  [c for c in df.columns if c in ("status_reposicao","status")]
    preco_cols = [c for c in df.columns if c in ("preco","preço","preco_cat","preço_cat")]

    if not sku_cols:
        raise RuntimeError("CATALOGO: coluna de SKU não encontrada (ex.: component_sku/sku).")

    out = pd.DataFrame()
    # LINHAS ATRIBUÍDAS LIMPAS:
    out["component_sku"] = df[sku_cols[0]].map(norm_sku)
    out["fornecedor"] = df[forn_cols[0]].astype(str).str.strip() if forn_cols else ""
    out["status_reposicao"] = df[status_cols[0]].astype(str).str.strip() if status_cols else ""

    if preco_cols:
        preco = br_to_float(df[preco_cols[0]])
    else:
        preco = pd.Series([0.0] * len(out), index=out.index, dtype=float)

    # SAÍDA FINAL COM P MAIÚSCULO, conforme análise:
    out["Preco"] = pd.to_numeric(preco, errors="coerce").fillna(0.0).astype(float)

    out = out[out["component_sku"].astype(str).str.len() > 0].drop_duplicates(subset=["component_sku"], keep="last").reset_index(drop=True)
    return out

def _normalize_kits(df_raw: pd.DataFrame) -> pd.DataFrame:
    """Gera DataFrame de Kits padronizado, usando lógica robusta."""
    df = _to_lc_columns(df_raw).copy()

    kit_cols  = [c for c in df.columns if c in ("kit_sku","sku_kit","parent_sku","sku_pai","kit")]
    comp_cols = [c for c in df.columns if "component" in c and "sku" in c] or \
                [c for c in df.columns if c in ("component_sku","sku_componente")]
    qtd_cols  = [c for c in df.columns if c in ("quantidade","qtd","qtde","qty")]

    if not (kit_cols and comp_cols and qtd_cols):
        return pd.DataFrame(columns=["kit_sku","component_sku","qty"])

    out = pd.DataFrame({
        "kit_sku": df[kit_cols[0]].map(norm_sku),
        "component_sku": df[comp_cols[0]].map(norm_sku),
        "qty": pd.to_numeric(br_to_float(df[qtd_cols[0]]), errors="coerce").fillna(0).astype(int)
    })
    
    out = out[(out["kit_sku"] != "") & (out["component_sku"] != "") & (out["qty"] >= 1)].drop_duplicates(subset=["kit_sku","component_sku"], keep="first")
    return out.reset_index(drop=True)


def _carregar_padrao_de_content(content_bytes: bytes) -> Catalogo:
    """Loader principal usando o PATCH hiper-defensivo do usuário (V11.4)."""
    if not content_bytes:
        raise RuntimeError("Arquivo de padrão vazio.")

    xls = pd.ExcelFile(io.BytesIO(content_bytes), engine="openpyxl")

    # A lógica de busca por aba e normalização de preço é feita internamente,
    # prevenindo a ambiguidade no Pandas.
    df_cat_raw  = _pick_sheet_ci(xls, "catalogo", "catalog", "cat")
    
    df_kits_raw = None
    try:
        df_kits_raw = _pick_sheet_ci(xls, "kits", "kit")
    except Exception:
        pass

    catalogo_simples = _normalize_catalogo(df_cat_raw)
    kits_reais = _normalize_kits(df_kits_raw) if df_kits_raw is not None else \
                 pd.DataFrame(columns=["kit_sku","component_sku","qty"])

    return Catalogo(catalogo_simples=catalogo_simples, kits_reais=kits_reais)

# ===================== MAPEAMENTO E CÁLCULO (O restante do código) =====================
def mapear_tipo(df: pd.DataFrame) -> str:
    cols = [c.lower() for c in df.columns]
    tem_sku_std  = any(c in {"sku","codigo","codigo_sku"} for c in cols) or any("sku" in c for c in cols)
    tem_vendas60 = any(c.startswith("vendas_60d") or c in {"vendas 60d","vendas_qtd_60d"} for c in cols)
    tem_qtd_livre= any(("qtde" in c) or ("quant" in c) or ("venda" in c) or ("order" in c) for c in cols)
    tem_estoque_full_like = any(("estoque" in c and "full" in c) or c=="estoque_full" for c in cols)
    tem_estoque_generico  = any(c in {"estoque_atual","qtd","quantidade"} or "estoque" in c for c in cols)
    tem_transito_like     = any(("transito" in c) or c in {"em_transito","em transito","em_transito_full","em_transito_do_anuncio"} for c in cols)
    tem_preco = any(c in {"preco","preco_compra","preco_medio","custo","custo_medio"} for c in cols)

    if tem_sku_std and (tem_vendas60 or tem_estoque_full_like or tem_transito_like): return "FULL"
    if tem_sku_std and tem_estoque_generico and tem_preco: return "FISICO"
    if tem_sku_std and tem_qtd_livre and not tem_preco: return "VENDAS"
    return "DESCONHECIDO"

def mapear_colunas(df: pd.DataFrame, tipo: str) -> pd.DataFrame:
    if tipo == "FULL":
        if "sku" in df.columns: df["SKU"] = df["sku"].map(norm_sku)
        elif "codigo" in df.columns: df["SKU"] = df["codigo"].map(norm_sku)
        elif "codigo_sku" in df.columns: df["SKU"] = df["codigo_sku"].map(norm_sku)
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
        sku_series = (df["sku"] if "sku" in df.columns else (df["codigo"] if "codigo" in df.columns else (df["codigo_sku"] if "codigo_sku" in df.columns else None)))
        if sku_series is None: cand = next((c for c in df.columns if "sku" in c.lower()), None)
        if cand is None: raise RuntimeError("FÍSICO inválido: não achei coluna de SKU.")
        sku_series = df.get(cand) if cand else sku_series
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
        if sku_col is None: raise RuntimeError("VENDAS inválido: não achei coluna de SKU.")
        df["SKU"] = df[sku_col].map(norm_sku)
        cand_qty = []
        for c in df.columns:
            cl = c.lower(); score = 0
            if "qtde" in cl: score += 3;
            if "quant" in cl: score += 2;
            if "venda" in cl: score += 1;
            if "order" in cl: score += 1
            if score > 0: cand_qty.append((score, c))
        if not cand_qty: raise RuntimeError("VENDAS inválido: não achei coluna de Quantidade.")
        cand_qty.sort(reverse=True); qcol = cand_qty[0][1]
        df["Quantidade"] = df[qcol].map(br_to_float).fillna(0).astype(int)
        return df[["SKU","Quantidade"]].copy()
    raise RuntimeError("Tipo de arquivo desconhecido.")
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
    shp = vendas_df.copy()
    ml_comp = explodir_por_kits(full[["SKU","Vendas_Qtd_60d"]].rename(columns={"SKU":"kit_sku","Vendas_Qtd_60d":"Qtd"}), kits,"kit_sku","Qtd").rename(columns={"Quantidade":"ML_60d"})
    shopee_comp = explodir_por_kits(shp[["SKU","Quantidade"]].rename(columns={"SKU":"kit_sku","Quantidade":"Qtd"}), kits,"kit_sku","Qtd").rename(columns={"Quantidade":"Shopee_60d"})
    cat_df = cat.catalogo_simples[["component_sku","fornecedor","status_reposicao","Preco"]].rename(columns={"component_sku":"SKU"})
    demanda = cat_df.merge(ml_comp, on="SKU", how="left").merge(shopee_comp, on="SKU", how="left")
    demanda[["ML_60d","Shopee_60d"]] = demanda[["ML_60d","Shopee_60d"]].fillna(0).astype(int)
    demanda["TOTAL_60d"] = np.maximum(demanda["ML_60d"] + demanda["Shopee_60d"], demanda["ML_60d"]).astype(int)
    fis = fisico_df.copy()
    fis["Estoque_Fisico"] = fis["Estoque_Fisico"].fillna(0).astype(int)
    fis["Preco_Fisico"] = fis["Preco"].fillna(0.0) 
    base = demanda.merge(fis.drop(columns=["Preco"], errors="ignore"), on="SKU", how="left")
    base["Estoque_Fisico"] = base["Estoque_Fisico"].fillna(0).astype(int)
    base["Preco_Fisico"] = base["Preco_Fisico"].fillna(0.0)
    base["Preco"] = base["Preco"].fillna(0.0)
    base["Preco"] = np.where( (base["Preco"] == 0.0) | pd.isna(base["Preco"]), base["Preco_Fisico"], base["Preco"] )
    fator = (1.0 + g/100.0) ** (h/30.0)
    fk = full.copy()
    fk["vendas_dia"] = fk["Vendas_Qtd_60d"] / 60.0
    fk["alvo"] = np.round(fk["vendas_dia"] * (LT + h) * fator).astype(int)
    fk["oferta"] = (fk["Estoque_Full"] + fk["Em_Transito"]).astype(int)
    fk["envio_desejado"] = (fk["alvo"] - fk["oferta"]).clip(lower=0).astype(int)
    necessidade = explodir_por_kits(fk[["SKU","envio_desejado"]].rename(columns={"SKU":"kit_sku","envio_desejado":"Qtd"}), kits,"kit_sku","Qtd").rename(columns={"Quantidade":"Necessidade"})
    base = base.merge(necessidade, on="SKU", how="left"); base["Necessidade"] = base["Necessidade"].fillna(0).astype(int)
    base["Demanda_dia"] = base["TOTAL_60d"] / 60.0; base["Reserva_30d"] = np.round(base["Demanda_dia"] * 30).astype(int)
    base["Folga_Fisico"] = (base["Estoque_Fisico"] - base["Reserva_30d"]).clip(lower=0).astype(int)
    base["Compra_Sugerida"] = (base["Necessidade"] - base["Folga_Fisico"]).clip(lower=0).astype(int)
    mask_nao = base["status_reposicao"].str.lower().str.contains("nao_repor", na=False); base.loc[mask_nao, "Compra_Sugerida"] = 0
    base["Valor_Compra_R$"] = (base["Compra_Sugerida"].astype(float) * base["Preco"].astype(float)).round(2)
    base["Vendas_h_ML"] = np.round(base["ML_60d"] * (h/60.0)).astype(int); base["Vendas_h_Shopee"] = np.round(base["Shopee_60d"] * (h/60.0)).astype(int)
    base = base.sort_values(["fornecedor","Valor_Compra_R$","SKU"], ascending=[True, False, True])
    df_final = base[[
        "SKU","fornecedor","Vendas_h_ML","Vendas_h_Shopee","Estoque_Fisico","Preco","Compra_Sugerida","Valor_Compra_R$",
        "ML_60d","Shopee_60d","TOTAL_60d","Reserva_30d","Folga_Fisico","Necessidade"
    ]].reset_index(drop=True)
    fis_unid  = int(fis.get("Estoque_Fisico", pd.Series([0])).sum())
    fis_valor = float((fis.get("Estoque_Fisico", pd.Series([0])) * fis.get("Preco_Fisico", pd.Series([0.0]))).sum())
    full_stock_comp = explodir_por_kits(full[["SKU","Estoque_Full"]].rename(columns={"SKU":"kit_sku","Estoque_Full":"Qtd"}), kits,"kit_sku","Qtd")
    full_stock_comp = full_stock_comp.merge(fis[["SKU","Preco_Fisico"]].rename(columns={"Preco_Fisico":"Preco"}), on="SKU", how="left")
    full_unid  = int(full["Estoque_Full"].sum())
    full_valor = float((full_stock_comp["Quantidade"].fillna(0) * full_stock_comp["Preco"].fillna(0.0)).sum())
    painel = {"full_unid": full_unid, "full_valor": full_valor, "fisico_unid": fis_unid, "fisico_valor": fis_valor}
    return df_final, painel
def exportar_xlsx(df_final: pd.DataFrame, h: int, params: dict, pendencias: list | None = None) -> bytes:
    int_cols = ["Vendas_h_ML","Vendas_h_Shopee","Estoque_Fisico","Compra_Sugerida","Reserva_30d","Folga_Fisico","Necessidade","ML_60d","Shopee_60d","TOTAL_60d"]
    for c in int_cols:
        if c in df_final.columns:
            df_final[c] = pd.to_numeric(df_final[c], errors='coerce').fillna(0).astype(int)
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as w:
        lista = df_final[df_final["Compra_Sugerida"] > 0].copy()
        lista.to_excel(w, sheet_name="Lista_Final", index=False)
    output.seek(0)
    return output.read()