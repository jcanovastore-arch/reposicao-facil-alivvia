# v4_api/engine_compras.py — MOTOR REAL COMPLETO

from dataclasses import dataclass
from typing import Dict, Tuple

import numpy as np
import pandas as pd


# ---------- Funções base ----------
def norm_sku(x):
    if pd.isna(x):
        return ""
    return str(x).strip().upper()


@dataclass
class Catalogo:
    simples: pd.DataFrame     # component_sku, fornecedor, status_reposicao
    kits: pd.DataFrame        # kit_sku, component_sku, qty


def explodir(df: pd.DataFrame, kits: pd.DataFrame, col_sku: str, col_qtd: str):
    base = df.copy()
    base["kit_sku"] = base[col_sku].map(norm_sku)
    base["Qtd"] = base[col_qtd].astype(int)

    merged = base.merge(kits, on="kit_sku", how="left")
    merged = merged.dropna(subset=["component_sku"])

    merged["qty"] = merged["qty"].astype(int)
    merged["quantidade"] = merged["Qtd"] * merged["qty"]

    out = merged.groupby("component_sku", as_index=False)["quantidade"].sum()
    out = out.rename(columns={"component_sku": "SKU", "quantidade": "Qtd"})
    return out


def construir_kits(cat: Catalogo):
    kits = cat.kits.copy()
    componentes = set(cat.simples["component_sku"].unique())
    kits_validos = set(kits["kit_sku"].unique())

    kits = kits[kits["component_sku"].isin(componentes)].copy()

    alias = []
    for c in componentes:
        c2 = norm_sku(c)
        if c2 not in kits_validos:
            alias.append((c2, c2, 1))

    if alias:
        alias_df = pd.DataFrame(alias, columns=["kit_sku", "component_sku", "qty"])
        kits = pd.concat([kits, alias_df], ignore_index=True)

    return kits.drop_duplicates()


# ---------- Motor real ----------
def calcular_compra(
    arquivos: Dict,
    horizonte: int,
    crescimento: float,
    leadtime: int
) -> Tuple[pd.DataFrame, Dict]:

    # === 1) LER CATÁLOGO ===
    catalogo = arquivos["catalogo"]["catalogo"]
    kits_raw = arquivos["catalogo"]["kits"]

    catalogo["component_sku"] = catalogo["component_sku"].map(norm_sku)
    kits_raw["kit_sku"] = kits_raw["kit_sku"].map(norm_sku)
    kits_raw["component_sku"] = kits_raw["component_sku"].map(norm_sku)

    cat = Catalogo(simples=catalogo, kits=kits_raw)
    kits = construir_kits(cat)

    # === 2) LER BASES ALIVVIA & JCA ===
    empresas = ["alivvia", "jca"]
    full, vendas, estoque = [], [], []

    for emp in empresas:
        emp_data = arquivos.get(emp, {})

        if "full" in emp_data:
            df = emp_data["full"]
            df["SKU"] = df["SKU"].map(norm_sku)
            full.append(df)

        if "vendas" in emp_data:
            df = emp_data["vendas"]
            df["SKU"] = df["SKU"].map(norm_sku)
            vendas.append(df)

        if "estoque" in emp_data:
            df = emp_data["estoque"]
            df["SKU"] = df["SKU"].map(norm_sku)
            estoque.append(df)

    full = pd.concat(full, ignore_index=True)
    vendas = pd.concat(vendas, ignore_index=True)
    estoque = pd.concat(estoque, ignore_index=True)

    # === 3) NORMALIZAR ===
    full["Vendas_Qtd_60d"] = full["Vendas_Qtd_60d"].astype(int)
    full["Estoque_Full"] = full["Estoque_Full"].astype(int)
    full["Em_Transito"] = full["Em_Transito"].astype(int)

    vendas["Quantidade"] = vendas["Quantidade"].astype(int)

    estoque["Estoque_Fisico"] = estoque["Estoque_Fisico"].fillna(0).astype(int)
    estoque["Preco"] = estoque["Preco"].fillna(0).astype(float)

    # === 4) EXPLODIR VENDAS ===
    ml_exp = explodir(
        full[["SKU", "Vendas_Qtd_60d"]].rename(columns={"SKU": "kit_sku", "Vendas_Qtd_60d": "Qtd"}),
        kits, "kit_sku", "Qtd"
    ).rename(columns={"Qtd": "ML_60d"})

    shp_exp = explodir(
        vendas[["SKU", "Quantidade"]].rename(columns={"SKU": "kit_sku", "Quantidade": "Qtd"}),
        kits, "kit_sku", "Qtd"
    ).rename(columns={"Qtd": "Shopee_60d"})

    # === 5) DEMANDA ===
    base = catalogo.copy()
    base = base.rename(columns={"component_sku": "SKU"})

    base = base.merge(ml_exp, on="SKU", how="left")
    base = base.merge(shp_exp, on="SKU", how="left")

    base["ML_60d"] = base["ML_60d"].fillna(0).astype(int)
    base["Shopee_60d"] = base["Shopee_60d"].fillna(0).astype(int)

    base["Vendas_Total_60d"] = base["ML_60d"] + base["Shopee_60d"]

    # === 6) ESTOQUE ===
    base = base.merge(estoque[["SKU", "Estoque_Fisico", "Preco"]], on="SKU", how="left")
    base = base.merge(full[["SKU", "Estoque_Full", "Em_Transito"]], on="SKU", how="left")

    base["Estoque_Fisico"] = base["Estoque_Fisico"].fillna(0).astype(int)
    base["Estoque_Full"] = base["Estoque_Full"].fillna(0).astype(int)
    base["Em_Transito"] = base["Em_Transito"].fillna(0).astype(int)
    base["Preco"] = base["Preco"].fillna(0).astype(float)

    # === 7) CÁLCULO DA NECESSIDADE ===
    fator = (1 + crescimento / 100) ** (horizonte / 30)

    full2 = full.copy()
    full2["vendas_dia"] = full2["Vendas_Qtd_60d"] / 60
    full2["target"] = np.round(full2["vendas_dia"] * (horizonte + leadtime) * fator).astype(int)
    full2["oferta"] = full2["Estoque_Full"] + full2["Em_Transito"]
    full2["envio"] = (full2["target"] - full2["oferta"]).clip(lower=0)

    nec = explodir(
        full2[["SKU", "envio"]].rename(columns={"SKU": "kit_sku", "envio": "Qtd"}),
        kits, "kit_sku", "Qtd"
    ).rename(columns={"Qtd": "Necessidade"})

    base = base.merge(nec, on="SKU", how="left")
    base["Necessidade"] = base["Necessidade"].fillna(0).astype(int)

    # FOLGA DO FÍSICO
    base["Demanda_dia"] = base["Vendas_Total_60d"] / 60
    base["Reserva_30d"] = np.round(base["Demanda_dia"] * 30).astype(int)
    base["Folga_Fisico"] = (base["Estoque_Fisico"] - base["Reserva_30d"]).clip(lower=0)

    # COMPRA FINAL
    base["Compra_Sugerida"] = (base["Necessidade"] - base["Folga_Fisico"]).clip(lower=0)
    base["Valor_Compra_R$"] = (base["Compra_Sugerida"] * base["Preco"]).round(2)

    # === 8) PAINEL ===
    painel = {
        "full_unid": int(base["Estoque_Full"].sum()),
        "full_valor": float((full2["envio"].sum()) if len(full2) else 0),
        "fisico_unid": int(base["Estoque_Fisico"].sum()),
        "fisico_valor": float((base["Estoque_Fisico"] * base["Preco"]).sum()),
    }

    final_cols = [
        "SKU", "fornecedor", "Vendas_Total_60d",
        "Estoque_Full", "Estoque_Fisico",
        "Compra_Sugerida", "Valor_Compra_R$"
    ]

    final = base[final_cols].sort_values("SKU").reset_index(drop=True)
    return final, painel
