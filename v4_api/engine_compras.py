# v4_api/engine_compras.py
# Motor de cálculo de reposição (sem UI)

from dataclasses import dataclass
from typing import Tuple, Dict

import numpy as np
import pandas as pd


def br_to_float(x):
    if pd.isna(x):
        return np.nan
    if isinstance(x, (int, float, np.integer, np.floating)):
        return float(x)
    s = str(x).strip()
    if s == "":
        return np.nan
    s = (
        s.replace("\u00a0", " ")
         .replace("R$", "")
         .replace(" ", "")
         .replace(".", "")
         .replace(",", ".")
    )
    try:
        return float(s)
    except Exception:
        return np.nan


def norm_sku(x: str) -> str:
    if pd.isna(x):
        return ""
    return str(x).strip().upper()


@dataclass
class Catalogo:
    catalogo_simples: pd.DataFrame
    kits_reais: pd.DataFrame


def explodir_por_kits(df: pd.DataFrame, kits: pd.DataFrame,
                      sku_col: str, qtd_col: str) -> pd.DataFrame:
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


def construir_kits_efetivo(cat: Catalogo) -> pd.DataFrame:
    kits = cat.kits_reais.copy()

    componentes_validos = set(cat.catalogo_simples["component_sku"].unique())
    kits_validos = set(kits["kit_sku"].unique())

    kits = kits[kits["component_sku"].isin(componentes_validos)].copy()

    alias = []
    for s in componentes_validos:
        s_norm = norm_sku(s)
        if s_norm and s_norm not in kits_validos:
            alias.append((s_norm, s_norm, 1))

    if alias:
        kits_df_alias = pd.DataFrame(alias, columns=["kit_sku", "component_sku", "qty"])
        kits = pd.concat([kits, kits_df_alias], ignore_index=True)

    kits = kits.drop_duplicates(subset=["kit_sku", "component_sku"], keep="first")
    return kits


def calcular_compra(
    arquivos: Dict,
    horizonte: int,
    crescimento: float,
    leadtime: int
) -> Tuple[pd.DataFrame, Dict]:

    # IMPORTANTE: você já usa os arquivos processados no supabase_client
    ali = arquivos.get("alivvia") or arquivos.get("ali") or arquivos.get("ALI")
    jca = arquivos.get("jca") or arquivos.get("JCA")

    # TODO → Aqui vamos implementar unificação ALI/JCA
    # Por enquanto, só devolvemos algo simples para testar:
    df = pd.DataFrame({
        "SKU": ["TESTE1", "TESTE2"],
        "Compra_Sugerida": [10, 20]
    })

    painel = {
        "full_unid": 0,
        "full_valor": 0,
        "fisico_unid": 0,
        "fisico_valor": 0
    }

    return df, painel
