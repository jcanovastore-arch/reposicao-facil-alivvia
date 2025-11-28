# v4_api/engine_compras.py
# Motor base simplificado para testar integração com a API

from dataclasses import dataclass
from typing import Dict, Tuple
import pandas as pd
import numpy as np


# ============================================================
# Funções auxiliares
# ============================================================

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


# ============================================================
# Funções de KIT (apenas estrutura, ainda não cálculo real)
# ============================================================

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


# ============================================================
# CALCULAR_COMPRA (versão simplificada para integração)
# ============================================================

def calcular_compra(
    arquivos: Dict,
    horizonte: int,
    crescimento: float,
    leadtime: int
) -> Tuple[pd.DataFrame, Dict]:
    """
    ESTA VERSÃO É SIMPLIFICADA PARA TESTAR A INTEGRAÇÃO.
    Ela só devolve uma tabela fixa, garantindo que o Lovable recebe resposta.
    """

    # Apenas imprime os arquivos recebidos (debug)
    print("=== ARQUIVOS RECEBIDOS NO MOTOR ===")
    print(arquivos)

    # TABELA FAKE (apenas para validar comunicação)
    df = pd.DataFrame({
        "SKU": ["TESTE1", "TESTE2", "TESTE3"],
        "Compra_Sugerida": [5, 12, 30]
    })

    # PAINEL FAKE (também só para validar)
    painel = {
        "full_unid": 10,
        "full_valor": 123.45,
        "fisico_unid": 50,
        "fisico_valor": 987.65
    }

    return df, painel
