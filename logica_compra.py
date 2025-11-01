# logica_compra.py - MÓDULO DE LÓGICA DE NEGÓCIOS V5.1
# Contém todas as funções de utilidade, leitura, mapeamento, explosão de kits e cálculo.
# Este arquivo é 100% livre de código Streamlit.

import io
import re
import hashlib
import datetime as dt
from dataclasses import dataclass
from typing import Optional, Tuple
import numpy as np
import pandas as pd
from unidecode import unidecode
import requests
from requests.adapters import HTTPAdapter, Retry

# ===================== CONFIG / HTTP =====================
DEFAULT_SHEET_ID = "1cTLARjq-B5g50dL6tcntg7lb_Iu0ta43"

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

# ===================== CÁLCULOS PRINCIPAIS =====================
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

    # COLUNAS FINAIS (Simplificadas, conforme solicitado)
    df_final = base[[
        "SKU","fornecedor",
        "Estoque_Fisico","Preco","Compra_Sugerida","Valor_Compra_R$",
        "ML_60d","Shopee_60d","TOTAL_60d"
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
def aggregate_data_for_conjunta_clean(
    full_A_df: pd.DataFrame, vend_A_df: pd.DataFrame, fisi_A_df: pd.DataFrame,
    full_J_df: pd.DataFrame, vend_J_df: pd.DataFrame, fisi_J_df: pd.DataFrame
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Agrega DFs de duas empresas sem depender de Streamlit."""
    
    # 1. Agregação FULL
    full_conjunta = pd.merge(full_A_df, full_J_df, on="SKU", how="outer", suffixes=("_A", "_J")).fillna(0)
    full_conjunta["Vendas_Qtd_60d"] = full_conjunta["Vendas_Qtd_60d_A"] + full_conjunta["Vendas_Qtd_60d_J"]
    full_conjunta["Estoque_Full"] = full_conjunta["Estoque_Full_A"] + full_conjunta["Estoque_Full_J"]
    full_conjunta["Em_Transito"] = full_conjunta["Em_Transito_A"] + full_conjunta["Em_Transito_J"]
    full_df_final = full_conjunta[["SKU", "Vendas_Qtd_60d", "Estoque_Full", "Em_Transito"]].copy()

    # 2. Agregação VENDAS (Shopee/MT)
    vend_conjunta = pd.merge(vend_A_df, vend_J_df, on="SKU", how="outer", suffixes=("_A", "_J")).fillna(0)
    vend_conjunta["Quantidade"] = vend_conjunta["Quantidade_A"] + vend_conjunta["Quantidade_J"]
    vend_df_final = vend_conjunta[["SKU", "Quantidade"]].copy()
    
    # 3. Agregação FÍSICO
    fisi_conjunta = pd.merge(fisi_A_df, fisi_J_df, on="SKU", how="outer", suffixes=("_A", "_J")).fillna(0)
    fisi_conjunta["Estoque_Fisico"] = fisi_conjunta["Estoque_Fisico_A"] + fisi_conjunta["Estoque_Fisico_J"]
    
    # Preço: Usa o preço mais alto/não zero.
    fisi_conjunta["Preco"] = np.where(fisi_conjunta["Preco_A"] > fisi_conjunta["Preco_J"], fisi_conjunta["Preco_A"], fisi_conjunta["Preco_J"])
    fisi_conjunta["Preco"] = np.where(fisi_conjunta["Preco"] == 0, np.maximum(fisi_conjunta["Preco_A"], fisi_conjunta["Preco_J"]), fisi_conjunta["Preco"])
    fisi_df_final = fisi_conjunta[["SKU", "Estoque_Fisico", "Preco"]].copy()

    return full_df_final, fisi_df_final, vend_df_final

def calcular_vendas_componente(full_df: pd.DataFrame, shp_df: pd.DataFrame, cat: Catalogo) -> pd.DataFrame:
    """Calcula a demanda de componentes a partir de vendas de kits."""
    kits = construir_kits_efetivo(cat)
    a = explodir_por_kits(full_df[["SKU","Vendas_Qtd_60d"]].rename(columns={"SKU":"kit_sku","Vendas_Qtd_60d":"Qtd"}), kits,"kit_sku","Qtd")
    a = a.rename(columns={"Quantidade":"ML_60d"})
    b = explodir_por_kits(shp_df[["SKU","Quantidade"]].rename(columns={"SKU":"kit_sku","Quantidade":"Qtd"}), kits,"kit_sku","Qtd")
    b = b.rename(columns={"Quantidade":"Shopee_60d"})
    out = pd.merge(a, b, on="SKU", how="outer").fillna(0)
    out["Demanda_60d"] = out["ML_60d"].astype(int) + out["Shopee_60d"].astype(int)
    return out[["SKU","Demanda_60d", "ML_60d", "Shopee_60d"]]

# ===================== EXPORT XLSX =====================
def sha256_of_csv(df: pd.DataFrame) -> str:
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    return hashlib.sha256(csv_bytes).hexdigest()

def exportar_xlsx(df_final: pd.DataFrame, h: int, params: dict, pendencias: list | None = None) -> bytes:
    """Função de exportação sem dependências de UI."""
    int_cols = [c for c in ["Estoque_Fisico","Compra_Sugerida","ML_60d","Shopee_60d","TOTAL_60d"] if c in df_final.columns]
    for c in int_cols:
        if c not in df_final.columns: continue
        bad = df_final.index[(df_final[c] < 0) | (df_final[c].astype(float) % 1 != 0)]
        if len(bad) > 0:
            linha = int(bad[0]) + 2
            sku = df_final.loc[bad[0], "SKU"] if "SKU" in df_final.columns else "?"
            raise RuntimeError(f"Auditoria: coluna '{c}' precisa ser inteiro ≥ 0. Ex.: linha {linha} (SKU={sku}).")

    calc = (df_final["Compra_Sugerida"] * df_final["Preco"]).round(2).values
    if not np.allclose(df_final["Valor_Compra_R$"].values, calc, atol=0.01):
        # Aumentei o atol (Absolute Tolerance) para lidar com pequenos erros de ponto flutuante, comum em arredondamentos do pandas
        bad = np.where(~np.isclose(df_final["Valor_Compra_R$"].values, calc, atol=0.01))[0]
        linha = int(bad[0]) + 2 if len(bad) else "?"
        sku = df_final.iloc[bad[0]]["SKU"] if len(bad) and "SKU" in df_final.columns else "?"
        raise RuntimeError(f"Auditoria: 'Valor_Compra_R$' ≠ 'Compra_Sugerida × Preco'. Ex.: linha {linha} (SKU={sku}).")

    hash_str = sha256_of_csv(df_final)
    output = io.BytesIO()
    
    try:
        # Tenta usar xlsxwriter (mais robusto)
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
    except:
        # Fallback para openpyxl
        with pd.ExcelWriter(output, engine="openpyxl") as w:
            lista = df_final[df_final["Compra_Sugerida"] > 0].copy()
            lista.to_excel(w, sheet_name="Lista_Final", index=False)
            
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