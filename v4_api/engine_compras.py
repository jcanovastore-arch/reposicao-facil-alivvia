# v4_api/engine_compras.py
# Motor de cálculo de reposição (sem UI)

from dataclasses import dataclass
from typing import Tuple, Dict

import numpy as np
import pandas as pd


# ============== FUNÇÕES BÁSICAS Q USAMOS NO CÁLCULO ==============

def br_to_float(x):
    """Converte string estilo BR para float (ex: 'R$ 1.234,56')."""
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
    """Normaliza SKU em maiúsculo e sem espaços extras."""
    if pd.isna(x):
        return ""
    return str(x).strip().upper()


@dataclass
class Catalogo:
    catalogo_simples: pd.DataFrame  # component_sku, fornecedor, status_reposicao
    kits_reais: pd.DataFrame        # kit_sku, component_sku, qty


# ===================== KITS (EXPLOSÃO) =====================

def explodir_por_kits(df: pd.DataFrame, kits: pd.DataFrame,
                      sku_col: str, qtd_col: str) -> pd.DataFrame:
    """
    Recebe vendas/estoque por SKU de KIT e explode para componentes,
    usando a tabela de kits (kit_sku, component_sku, qty).
    """
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
    """
    Normaliza a tabela de kits:
      - garante apenas componentes válidos (que aparecem no catálogo),
      - cria alias 1:1 para cada componente simples que não é kit.
    Retorna DataFrame com colunas: kit_sku, component_sku, qty
    """
    kits = cat.kits_reais.copy()

    componentes_validos = set(cat.catalogo_simples["component_sku"].unique())
    kits_validos = set(kits["kit_sku"].unique())

    # 1. Mantém apenas linhas cujos componentes são válidos no catálogo
    kits = kits[kits["component_sku"].isin(componentes_validos)].copy()

    # 2. Cria alias simples (cada componente vira um "kit" de 1x ele mesmo)
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


# ===================== CÁLCULO PRINCIPAL =====================

def calcular_compra(
    full_df: pd.DataFrame,
    fisico_df: pd.DataFrame,
    vendas_df: pd.DataFrame,
    catalogo_df: pd.DataFrame,
    kits_df: pd.DataFrame,
    h: int = 60,
    g: float = 0.0,
    LT: int = 0,
) -> Tuple[pd.DataFrame, Dict]:
    """
    Reproduz a lógica de cálculo atual, mas sem Streamlit nem estado global.

    Espera dataframes já com estas colunas:
      full_df:   SKU, Vendas_Qtd_60d, Estoque_Full, Em_Transito
      fisico_df: SKU, Estoque_Fisico, Preco
      vendas_df: SKU, Quantidade
      catalogo_df: component_sku, fornecedor, status_reposicao
      kits_df:   kit_sku, component_sku, qty
    """

    # 0. Monta objeto Catalogo + kits efetivos
    cat = Catalogo(
        catalogo_simples=catalogo_df.copy(),
        kits_reais=kits_df.copy()
    )
    kits = construir_kits_efetivo(cat)

    # 1. NORMALIZA BASES
    full = full_df.copy()
    full["SKU"] = full["SKU"].map(norm_sku)
    full["Vendas_Qtd_60d"] = full["Vendas_Qtd_60d"].astype(int)
    full["Estoque_Full"] = full["Estoque_Full"].astype(int)
    full["Em_Transito"] = full["Em_Transito"].astype(int)

    shp = vendas_df.copy()
    shp["SKU"] = shp["SKU"].map(norm_sku)
    shp["Quantidade_60d"] = shp["Quantidade"].astype(int)

    # 2. EXPLODE VENDAS FULL/SHOPEE PARA COMPONENTES
    ml_comp = explodir_por_kits(
        full[["SKU", "Vendas_Qtd_60d"]].rename(columns={"SKU": "kit_sku", "Vendas_Qtd_60d": "Qtd"}),
        kits, "kit_sku", "Qtd"
    ).rename(columns={"Quantidade": "ML_60d"})

    shopee_comp = explodir_por_kits(
        shp[["SKU", "Quantidade_60d"]].rename(columns={"SKU": "kit_sku", "Quantidade_60d": "Qtd"}),
        kits, "kit_sku", "Qtd"
    ).rename(columns={"Quantidade": "Shopee_60d"})

    cat_df = catalogo_df[["component_sku", "fornecedor", "status_reposicao"]].rename(
        columns={"component_sku": "SKU"}
    )

    # 3. MONTA DEMANDA
    demanda = cat_df.merge(ml_comp, on="SKU", how="left").merge(shopee_comp, on="SKU", how="left")
    demanda[["ML_60d", "Shopee_60d"]] = demanda[["ML_60d", "Shopee_60d"]].fillna(0).astype(int)
    demanda["TOTAL_60d"] = np.maximum(
        demanda["ML_60d"] + demanda["Shopee_60d"],
        demanda["ML_60d"]
    ).astype(int)
    demanda["Vendas_Total_60d"] = demanda["ML_60d"] + demanda["Shopee_60d"]

    # 4. ESTOQUE FÍSICO
    fis = fisico_df.copy()
    fis["SKU"] = fis["SKU"].map(norm_sku)
    fis["Estoque_Fisico"] = fis["Estoque_Fisico"].fillna(0).astype(int)
    fis["Preco"] = fis["Preco"].fillna(0.0)

    base = demanda.merge(fis, on="SKU", how="left")
    base["Estoque_Fisico"] = base["Estoque_Fisico"].fillna(0).astype(int)
    base["Preco"] = base["Preco"].fillna(0.0)

    # 5. MERGE COM FULL
    full_simple = full[["SKU", "Estoque_Full", "Em_Transito"]].copy()
    base = base.merge(full_simple, on="SKU", how="left")
    base["Estoque_Full"] = base["Estoque_Full"].fillna(0).astype(int)
    base["Em_Transito"] = base["Em_Transito"].fillna(0).astype(int)

    # 6. CÁLCULO DE NECESSIDADE (TARGET)
    fator = (1.0 + g / 100.0) ** (h / 30.0)

    fk = full.copy()
    fk["vendas_dia"] = fk["Vendas_Qtd_60d"] / 60.0
    fk["alvo"] = np.round(fk["vendas_dia"] * (LT + h) * fator).astype(int)
    fk["oferta"] = (full["Estoque_Full"] + full["Em_Transito"]).astype(int)
    fk["envio_desejado"] = (fk["alvo"] - fk["oferta"]).clip(lower=0).astype(int)

    necessidade = explodir_por_kits(
        fk[["SKU", "envio_desejado"]].rename(columns={"SKU": "kit_sku", "envio_desejado": "Qtd"}),
        kits, "kit_sku", "Qtd"
    ).rename(columns={"Quantidade": "Necessidade"})

    base = base.merge(necessidade, on="SKU", how="left")
    base["Necessidade"] = base["Necessidade"].fillna(0).astype(int)

    base["Demanda_dia"] = base["TOTAL_60d"] / 60.0
    base["Reserva_30d"] = np.round(base["Demanda_dia"] * 30).astype(int)
    base["Folga_Fisico"] = (base["Estoque_Fisico"] - base["Reserva_30d"]).clip(lower=0).astype(int)

    base["Compra_Sugerida"] = (base["Necessidade"] - base["Folga_Fisico"]).clip(lower=0).astype(int)
    base["Valor_Compra_R$"] = (
        base["Compra_Sugerida"].astype(float) * base["Preco"].astype(float)
    ).round(2)

    # 7. SELEÇÃO DAS COLUNAS FINAIS
    df_final = base[
        [
            "SKU",
            "fornecedor",
            "Vendas_Total_60d",
            "Estoque_Full",
            "Estoque_Fisico",
            "Preco",
            "Compra_Sugerida",
            "Valor_Compra_R$",
            "ML_60d",
            "Shopee_60d",
            "TOTAL_60d",
            "Reserva_30d",
            "Folga_Fisico",
            "Necessidade",
            "Em_Transito",
        ]
    ].reset_index(drop=True)

    # 8. PAINEL RESUMO (mesma ideia do app atual)
    fis_unid = int(fis["Estoque_Fisico"].sum())
    fis_valor = float((fis["Estoque_Fisico"] * fis["Preco"]).sum())

    full_stock_comp = explodir_por_kits(
        full[["SKU", "Estoque_Full"]].rename(columns={"SKU": "kit_sku", "Estoque_Full": "Qtd"}),
        kits, "kit_sku", "Qtd"
    )
    full_stock_comp = full_stock_comp.merge(fis[["SKU", "Preco"]], on="SKU", how="left")
    full_unid = int(full["Estoque_Full"].sum())
    full_valor = float(
        (full_stock_comp["Quantidade"].fillna(0) * full_stock_comp["Preco"].fillna(0.0)).sum()
    )

    painel = {
        "full_unid": full_unid,
        "full_valor": full_valor,
        "fisico_unid": fis_unid,
        "fisico_valor": fis_valor,
    }

    return df_final, painel
