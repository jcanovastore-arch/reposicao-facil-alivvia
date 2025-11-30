# v4_api/engine_compras.py
# Motor de compra final — usando apenas o catálogo do banco

from typing import Dict, Tuple
import pandas as pd
import numpy as np

from supabase import create_client
import os


# ============================================================
# 1) CONEXÃO COM SUPABASE
# ============================================================

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


# ============================================================
# 2) FUNÇÕES AUXILIARES
# ============================================================

def norm_sku(x):
    if pd.isna(x):
        return ""
    return str(x).strip().upper()


def br_to_float(x):
    if pd.isna(x):
        return np.nan
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).replace("R$", "").replace(".", "").replace(",", ".").strip()
    try:
        return float(s)
    except:
        return np.nan


# ============================================================
# 3) CARREGAR CATÁLOGO DIRETO DO BANCO
# ============================================================

def carregar_catalogo_do_banco():
    """
    Carrega produtos simples, kits e componentes diretamente do Supabase.
    Nada mais vem de planilhas externas.
    """

    # Produtos simples
    simples = supabase.table("produtos_simples").select("*").eq("ativo", True).execute()
    df_simples = pd.DataFrame(simples.data) if simples.data else pd.DataFrame()

    # Kits
    kits = supabase.table("produtos_kits").select("*").eq("ativo", True).execute()
    df_kits = pd.DataFrame(kits.data) if kits.data else pd.DataFrame()

    # Componentes dos kits
    componentes = supabase.table("produtos_kits_componentes").select("*").execute()
    df_comp = pd.DataFrame(componentes.data) if componentes.data else pd.DataFrame()

    return df_simples, df_kits, df_comp


# ============================================================
# 4) EXPLODIR KITS (transformar kits → SKUs simples)
# ============================================================

def explodir_kits(df_vendas: pd.DataFrame, df_kits: pd.DataFrame, df_comp: pd.DataFrame):
    """
    Converte vendas de kits em vendas de componentes.
    """
    if df_kits.empty or df_comp.empty:
        return df_vendas  # não há kits cadastrados

    df_vendas = df_vendas.copy()
    df_vendas["SKU"] = df_vendas["SKU"].map(norm_sku)

    # Merge vendas com componentes de kits
    merged = df_vendas.merge(
        df_comp,
        left_on="SKU",
        right_on="kit_sku",
        how="left"
    )

    # Se não for kit → quantidade explode igual
    merged["qty_final"] = merged["quantidade"] * merged["qty_por_kit"].fillna(1)

    # Se não tinha componente, mantém SKU simples
    merged["SKU_FINAL"] = merged["component_sku"].fillna(merged["SKU"])

    out = (
        merged.groupby("SKU_FINAL", as_index=False)["qty_final"]
        .sum()
        .rename(columns={"SKU_FINAL": "SKU", "qty_final": "quantidade"})
    )

    return out


# ============================================================
# 5) MOTOR DE COMPRA
# ============================================================

def calcular_compra(
    arquivos: Dict,
    horizonte: int,
    crescimento: float,
    leadtime: int
) -> Tuple[Dict, Dict]:

    # --------------------------------------------------------
    # 5.1 Carregar catálogo do banco
    # --------------------------------------------------------
    df_simples, df_kits, df_comp = carregar_catalogo_do_banco()

    # --------------------------------------------------------
    # 5.2 Carregar arquivos enviados pelo usuário (full, físico, vendas)
    # --------------------------------------------------------
    alivvia = arquivos.get("alivvia", {})
    jca = arquivos.get("jca", {})

    # Cada empresa fornece: full, fisico, vendas
    empresas = {
        "ALIVVIA": alivvia,
        "JCA": jca
    }

    resultado_empresas = {}

    for nome_empresa, dados in empresas.items():

        df_full = dados.get("full")
        df_fisico = dados.get("fisico")
        df_vendas = dados.get("vendas")

        if df_full is None or df_fisico is None or df_vendas is None:
            resultado_empresas[nome_empresa] = {
                "tabela": [],
                "painel": {"estoque_full": 0, "estoque_fisico": 0}
            }
            continue

        # Normalizar SKUs
        df_full["SKU"] = df_full["SKU"].map(norm_sku)
        df_fisico["SKU"] = df_fisico["SKU"].map(norm_sku)
        df_vendas["SKU"] = df_vendas["SKU"].map(norm_sku)

        # Explodir kits nas vendas
        df_vendas_simples = explodir_kits(df_vendas, df_kits, df_comp)

        # Quantidade/dia
        df_vendas_simples["media_dia"] = df_vendas_simples["quantidade"] / horizonte
        df_vendas_simples["media_dia"] *= (1 + crescimento)

        # Merge final
        df = df_simples.merge(df_full[["SKU", "ESTOQUE"]], on="SKU", how="left")
        df = df.merge(df_fisico[["SKU", "ESTOQUE"]], on="SKU", how="left", suffixes=("_full", "_fisico"))
        df = df.merge(df_vendas_simples[["SKU", "media_dia"]], on="SKU", how="left")

        df["ESTOQUE_full"] = df["ESTOQUE_full"].fillna(0)
        df["ESTOQUE_fisico"] = df["ESTOQUE_fisico"].fillna(0)
        df["media_dia"] = df["media_dia"].fillna(0)

        df["demanda_total"] = df["media_dia"] * leadtime

        df["estoque_total"] = df["ESTOQUE_full"] + df["ESTOQUE_fisico"]
        df["compra_sugerida"] = np.maximum(df["demanda_total"] - df["estoque_total"], 0).round()

        # Salvar resultado
        resultado_empresas[nome_empresa] = {
            "tabela": df.to_dict(orient="records"),
            "painel": {
                "estoque_full": df["ESTOQUE_full"].sum(),
                "estoque_fisico": df["ESTOQUE_fisico"].sum()
            }
        }

    return resultado_empresas, {"status": "ok"}
