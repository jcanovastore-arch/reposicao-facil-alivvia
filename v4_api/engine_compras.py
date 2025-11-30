# v4_api/engine_compras.py

from dataclasses import dataclass
from typing import Dict, Tuple
import numpy as np
import pandas as pd


def norm_sku(x):
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


# ===========================
# CÁLCULO POR EMPRESA
# ===========================

def calcular_empresa(full_df, fisico_df, vendas_df, catalogo_df, kits_df, h, g, LT):

    cat = Catalogo(catalogo_simples=catalogo_df.copy(),
                   kits_reais=kits_df.copy())

    kits = construir_kits_efetivo(cat)

    # NORMALIZAÇÃO
    full = full_df.copy()
    full["SKU"] = full["SKU"].map(norm_sku)
    full["Vendas_Qtd_60d"] = full["Vendas_Qtd_60d"].fillna(0).astype(int)
    full["Estoque_Full"] = full["Estoque_Full"].fillna(0).astype(int)
    full["Em_Transito"] = full["Em_Transito"].fillna(0).astype(int)

    shp = vendas_df.copy()
    shp["SKU"] = shp["SKU"].map(norm_sku)
    shp["Quantidade_60d"] = shp["Quantidade"].fillna(0).astype(int)

    # Explosão FULL
    ml_comp = explodir_por_kits(
        full[["SKU", "Vendas_Qtd_60d"]].rename(columns={"SKU": "kit_sku", "Vendas_Qtd_60d": "Qtd"}),
        kits, "kit_sku", "Qtd"
    ).rename(columns={"Quantidade": "ML_60d"})

    # Explosão Shopee
    shopee_comp = explodir_por_kits(
        shp[["SKU", "Quantidade_60d"]].rename(columns={"SKU": "kit_sku", "Quantidade_60d": "Qtd"}),
        kits, "kit_sku", "Qtd"
    ).rename(columns={"Quantidade": "Shopee_60d"})

    # Catálogo normalizado
    cat_df = catalogo_df[["component_sku", "fornecedor"]].rename(
        columns={"component_sku": "SKU"}
    )

    # Monta demandas
    demanda = (
        cat_df
        .merge(ml_comp, on="SKU", how="left")
        .merge(shopee_comp, on="SKU", how="left")
    )

    demanda["ML_60d"] = demanda["ML_60d"].fillna(0).astype(int)
    demanda["Shopee_60d"] = demanda["Shopee_60d"].fillna(0).astype(int)
    demanda["Vendas_Total_60d"] = demanda["ML_60d"] + demanda["Shopee_60d"]

    # Estoque físico
    fis = fisico_df.copy()
    fis["SKU"] = fis["SKU"].map(norm_sku)
    fis["Estoque_Fisico"] = fis["Estoque_Fisico"].fillna(0).astype(int)
    fis["Preco"] = fis["Preco"].fillna(0.0).astype(float)

    base = demanda.merge(fis, on="SKU", how="left")
    base["Estoque_Fisico"] = base["Estoque_Fisico"].fillna(0).astype(int)

    # Merge com FULL
    base = base.merge(
        full[["SKU", "Estoque_Full"]],
        on="SKU", how="left"
    )
    base["Estoque_Full"] = base["Estoque_Full"].fillna(0).astype(int)

    # Cálculo real da compra sugerida
    base["Compra_Sugerida"] = (
        (base["Vendas_Total_60d"] - base["Estoque_Fisico"] - base["Estoque_Full"])
        .clip(lower=0)
        .astype(int)
    )

    base["Valor_Compra_R$"] = (base["Compra_Sugerida"] * base["Preco"]).round(2)

    # COLUNAS FINAIS
    final = base[
        [
            "SKU",
            "fornecedor",
            "Vendas_Total_60d",
            "Estoque_Full",
            "Estoque_Fisico",
            "Compra_Sugerida",
            "Preco",
            "Valor_Compra_R$"
        ]
    ].copy()

    return final


# ===========================
# CÁLCULO COMPLETO
# ===========================

def calcular_compra(arquivos: Dict, horizonte, crescimento, leadtime):

    ali = arquivos.get("alivvia")
    jca = arquivos.get("jca")

    # Tabelas de entrada
    full_ali = ali["full"]
    vendas_ali = ali["vendas"]
    estoque_ali = ali["estoque"]

    full_jca = jca["full"]
    vendas_jca = jca["vendas"]
    estoque_jca = jca["estoque"]

    catalogo = arquivos["catalogo"]
    kits = arquivos["kits"]

    tabela_ali = calcular_empresa(
        full_ali, estoque_ali, vendas_ali, catalogo, kits,
        horizonte, crescimento, leadtime
    )

    tabela_jca = calcular_empresa(
        full_jca, estoque_jca, vendas_jca, catalogo, kits,
        horizonte, crescimento, leadtime
    )

    # Painel futuro, por enquanto retornamos vazio
    painel = {}

    return {
        "alivvia": tabela_ali.to_dict(orient="records"),
        "jca": tabela_jca.to_dict(orient="records"),
        "painel": painel
    }
