# orion/etl/mapeadores.py
# ETL: normalizações e mapeamento de colunas/tipos (FULL/FISICO/VENDAS)

from __future__ import annotations
import pandas as pd
import numpy as np
from unidecode import unidecode

# ------------------ Normalizações básicas ------------------

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

def norm_sku(x: str) -> str:
    if pd.isna(x):
        return ""
    return unidecode(str(x)).strip().upper()

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

# ------------------ Mapeamento de tipo ------------------

def mapear_tipo(df: pd.DataFrame) -> str:
    cols = [c.lower() for c in df.columns]
    tem_sku = any("sku" in c for c in cols)
    tem_v60 = any(c.startswith("vendas_60d") or c in {"vendas 60d", "vendas_qtd_60d"} for c in cols)
    tem_estoque_full = any(("estoque" in c and "full" in c) or c == "estoque_full" for c in cols)
    tem_transito = any(("transito" in c) or c in {"em_transito", "em transito", "em_transito_full"} for c in cols)
    tem_estoque_generico = any(c in {"estoque_atual", "qtd", "quantidade"} or "estoque" in c for c in cols)
    tem_preco = any(c in {"preco", "preco_compra", "custo", "custo_medio", "preco_medio", "preco_unitario"} for c in cols)

    if tem_sku and (tem_v60 or tem_estoque_full or tem_transito):
        return "FULL"
    if tem_sku and tem_estoque_generico and tem_preco:
        return "FISICO"
    if tem_sku and not tem_preco:
        return "VENDAS"
    return "DESCONHECIDO"

# ------------------ Mapeamento de colunas ------------------

def mapear_colunas(df: pd.DataFrame, tipo: str) -> pd.DataFrame:
    # Mantém comportamento idêntico ao original

    if tipo == "FULL":
        if "sku" in df.columns:
            df["SKU"] = df["sku"].map(norm_sku)
        elif "codigo" in df.columns:
            df["SKU"] = df["codigo"].map(norm_sku)
        elif "codigo_sku" in df.columns:
            df["SKU"] = df["codigo_sku"].map(norm_sku)
        else:
            raise RuntimeError("FULL inválido: precisa de SKU/codigo.")
        c_v = [c for c in df.columns if c in ["vendas_qtd_60d", "vendas_60d", "vendas 60d"] or c.startswith("vendas_60d")]
        if not c_v:
            raise RuntimeError("FULL inválido: faltou Vendas_60d.")
        df["Vendas_Qtd_60d"] = pd.to_numeric(df[c_v[0]].map(br_to_float), errors="coerce").fillna(0).astype(int)
        c_e = [c for c in df.columns if c in ["estoque_full", "estoque_atual"] or ("estoque" in c and "full" in c)]
        if not c_e:
            raise RuntimeError("FULL inválido: faltou Estoque_Full.")
        df["Estoque_Full"] = pd.to_numeric(df[c_e[0]].map(br_to_float), errors="coerce").fillna(0).astype(int)
        c_t = [c for c in df.columns if c in ["em_transito", "em transito", "em_transito_full"] or ("transito" in c)]
        df["Em_Transito"] = pd.to_numeric(df[c_t[0]].map(br_to_float), errors="coerce").fillna(0).astype(int) if c_t else 0
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
                raise RuntimeError("FÍSICO inválido: não achei SKU.")
            sku_series = df[cand]
        df["SKU"] = sku_series.map(norm_sku)
        c_q = [c for c in df.columns if c in ["estoque_atual", "qtd", "quantidade"] or ("estoque" in c)]
        if not c_q:
            raise RuntimeError("FÍSICO inválido: faltou Estoque.")
        df["Estoque_Fisico"] = pd.to_numeric(df[c_q[0]].map(br_to_float), errors="coerce").fillna(0).astype(int)
        c_p = [c for c in df.columns if c in ["preco", "preco_compra", "custo", "custo_medio", "preco_medio", "preco_unitario"]]
        if not c_p:
            raise RuntimeError("FÍSICO inválido: faltou Preço/Custo.")
        df["Preco"] = pd.to_numeric(df[c_p[0]].map(br_to_float), errors="coerce").fillna(0.0)
        return df[["SKU", "Estoque_Fisico", "Preco"]].copy()

    if tipo == "VENDAS":
        sku_col = next((c for c in df.columns if "sku" in c.lower()), None)
        if not sku_col:
            raise RuntimeError("VENDAS inválido: não achei SKU.")
        df["SKU"] = df[sku_col].map(norm_sku)
        cand_qty = []
        for c in df.columns:
            cl = c.lower()
            score = 0
            if "qtde" in cl: score += 3
            if "quant" in cl: score += 2
            if "venda" in cl: score += 1
            if "order" in cl: score += 1
            if score > 0:
                cand_qty.append((score, c))
        if not cand_qty:
            raise RuntimeError("VENDAS inválido: não achei Quantidade.")
        cand_qty.sort(reverse=True)
        qcol = cand_qty[0][1]
        df["Quantidade"] = pd.to_numeric(df[qcol].map(br_to_float), errors="coerce").fillna(0).astype(int)
        return df[["SKU", "Quantidade"]].copy()

    raise RuntimeError("Tipo desconhecido.")
