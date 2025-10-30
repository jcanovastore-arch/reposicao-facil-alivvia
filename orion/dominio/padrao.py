# orion/dominio/padrao.py
# Padrão KITS/CAT e dataclass Catalogo (sem mudança de comportamento)

from __future__ import annotations
from dataclasses import dataclass
from typing import Optional
import io
import pandas as pd

from orion.etl.mapeadores import norm_header, normalize_cols

@dataclass
class Catalogo:
    # component_sku, fornecedor, status_reposicao
    catalogo_simples: pd.DataFrame
    # kit_sku, component_sku, qty
    kits_reais: pd.DataFrame

def _carregar_padrao_de_content(content: bytes) -> "Catalogo":
    xls = pd.ExcelFile(io.BytesIO(content))

    def load_sheet(opts):
        for n in opts:
            if n in xls.sheet_names:
                return pd.read_excel(xls, n, dtype=str, keep_default_na=False)
        raise RuntimeError(f"Aba não encontrada. Esperado uma de {opts}. Abas: {xls.sheet_names}")

    df_kits = load_sheet(["KITS", "KITS_REAIS", "kits", "kits_reais"]).copy()
    df_cat  = load_sheet(["CATALOGO_SIMPLES", "CATALOGO", "catalogo_simples", "catalogo"]).copy()

    # KITS
    df_kits = normalize_cols(df_kits)
    m = {}
    for alvo, cand in {
        "kit_sku": ["kit_sku", "kit", "sku_kit"],
        "component_sku": ["component_sku", "componente", "sku_componente", "component"],
        "qty": ["qty", "qtd", "quantidade", "qty_por_kit", "qtd_por_kit", "quantidade_por_kit"],
    }.items():
        for c in cand:
            if c in df_kits.columns:
                m[c] = alvo
                break
    df_kits = df_kits.rename(columns=m)
    for col in ["kit_sku", "component_sku", "qty"]:
        if col not in df_kits.columns:
            raise RuntimeError("KITS precisa de 'kit_sku', 'component_sku', 'qty'.")
    df_kits = df_kits[["kit_sku", "component_sku", "qty"]].copy()
    df_kits["kit_sku"] = df_kits["kit_sku"].astype(str).str.strip().str.upper()
    df_kits["component_sku"] = df_kits["component_sku"].astype(str).str.strip().str.upper()
    df_kits["qty"] = pd.to_numeric(df_kits["qty"], errors="coerce").fillna(0).astype(int)
    df_kits = df_kits[df_kits["qty"] >= 1].drop_duplicates(subset=["kit_sku", "component_sku"])

    # CATALOGO
    df_cat = normalize_cols(df_cat)
    m = {}
    for alvo, cand in {
        "component_sku": ["component_sku", "sku", "produto", "item", "codigo", "sku_componente"],
        "fornecedor": ["fornecedor", "supplier", "fab", "marca"],
        "status_reposicao": ["status_reposicao", "status", "reposicao_status"],
    }.items():
        for c in cand:
            if c in df_cat.columns:
                m[c] = alvo
                break
    df_cat = df_cat.rename(columns=m)
    if "component_sku" not in df_cat.columns:
        raise ValueError("CATALOGO precisa da coluna 'component_sku'.")
    if "fornecedor" not in df_cat.columns:
        df_cat["fornecedor"] = ""
    if "status_reposicao" not in df_cat.columns:
        df_cat["status_reposicao"] = ""
    df_cat["component_sku"] = df_cat["component_sku"].astype(str).str.strip().str.upper()
    df_cat["fornecedor"] = df_cat["fornecedor"].fillna("")
    df_cat["status_reposicao"] = df_cat["status_reposicao"].fillna("")
    df_cat = df_cat.drop_duplicates(subset=["component_sku"], keep="last")

    return Catalogo(df_cat, df_kits)

def carregar_padrao_do_xlsx(sheet_id: str, baixar_xlsx_do_sheets) -> Catalogo:
    """
    Mantém a assinatura de uso no app: recebe sheet_id e uma função downloader.
    """
    content = baixar_xlsx_do_sheets(sheet_id)
    return _carregar_padrao_de_content(content)
