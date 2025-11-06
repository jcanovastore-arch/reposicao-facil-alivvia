# ==== PATCH: Leitura hiper-defensiva do Padrão (KITS/CAT) ====
# Cole este bloco em logica_compra.py (substitui a função antiga _carregar_padrao_de_content
# e adiciona helpers necessários). Mantém o contrato: retorna Catalogo(catalogo_simples, kits_reais).

import io
import re
import numpy as np
import pandas as pd
from dataclasses import dataclass

# Se já existir @dataclass Catalogo no arquivo, mantenha a sua versão e remova esta.
@dataclass
class Catalogo:
    catalogo_simples: pd.DataFrame
    kits_reais: pd.DataFrame

# ----------------- Helpers robustos -----------------
_BR_MONEY_RE = re.compile(r"[^\d,.-]+")

def br_to_float(series_or_scalar):
    """Converte valores BR ('R$ 12,90') em float. Aceita escalar ou Series."""
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

def _to_lc_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Colunas em minúsculas, sem espaços extras."""
    m = {c: c.strip().lower() for c in df.columns}
    return df.rename(columns=m)

def _pick_sheet_ci(xls: pd.ExcelFile, *candidates) -> pd.DataFrame:
    """Escolhe aba por substring case-insensitive. Ex.: _pick_sheet_ci(xls,'catalog','catalogo')."""
    names = {name.lower(): name for name in xls.sheet_names}
    for cand in candidates:
        cand_lc = cand.lower()
        for lc, real in names.items():
            if cand_lc in lc:
                return xls.parse(real, dtype=str, engine="openpyxl")
    raise RuntimeError(f"Aba não encontrada: tente nomes contendo {candidates}")

# ----------------- Normalizações de dados -----------------
def _normalize_catalogo(df_raw: pd.DataFrame) -> pd.DataFrame:
    """Gera DataFrame com colunas padronizadas:
       ['component_sku','fornecedor','status_reposicao','preco']"""
    df = _to_lc_columns(df_raw).copy()

    # Mapas de possíveis nomes
    sku_cols = [c for c in df.columns if "component" in c and "sku" in c] or \
               [c for c in df.columns if c in ("component_sku","sku","código (sku)","codigo (sku)","codigo","código")]
    forn_cols = [c for c in df.columns if c in ("fornecedor","supplier","vendor")]
    status_cols = [c for c in df.columns if "status" in c and "repos" in c] or \
                  [c for c in df.columns if c in ("status_reposicao","status reposicao","status")]
    preco_cols = [c for c in df.columns if c in ("preco","preço","preco_cat","preço_cat")]

    if not sku_cols:
        raise RuntimeError("CATALOGO: coluna de SKU não encontrada (ex.: component_sku/sku).")

    out = pd.DataFrame()
    out["component_sku"]    = df[sku_cols[0]].astype(str).str.strip()
    out["fornecedor"]       = df[forn_cols[0]].astype(str).str.strip() if forn_cols else ""
    out["status_reposicao"] = df[status_cols[0]].astype(str).str.strip() if status_cols else ""

    if preco_cols:
        preco = br_to_float(df[preco_cols[0]])
    else:
        preco = pd.Series([0.0] * len(out), index=out.index, dtype=float)

    # Garante float, sem NaN
    out["preco"] = pd.to_numeric(preco, errors="coerce").fillna(0.0).astype(float)

    # Remove linhas vazias de SKU
    out = out[out["component_sku"].astype(str).str.len() > 0].reset_index(drop=True)
    return out

def _normalize_kits(df_raw: pd.DataFrame) -> pd.DataFrame:
    """Gera DataFrame com colunas padronizadas para kits reais:
       ['kit_sku','component_sku','quantidade']"""
    df = _to_lc_columns(df_raw).copy()

    # Tenta alguns nomes comuns
    kit_cols  = [c for c in df.columns if c in ("kit_sku","sku_kit","parent_sku","sku pai","sku_pai","sku kit","kit")]
    comp_cols = [c for c in df.columns if "component" in c and "sku" in c] or \
                [c for c in df.columns if c in ("component_sku","sku_componente","sku comp","comp_sku")]
    qtd_cols  = [c for c in df.columns if c in ("quantidade","qtd","qtde","qty")]

    if not (kit_cols and comp_cols and qtd_cols):
        # Se não for uma planilha de kits válida, retorna vazio — nunca explode.
        return pd.DataFrame(columns=["kit_sku","component_sku","quantidade"])

    out = pd.DataFrame({
        "kit_sku":       df[kit_cols[0]].astype(str).str.strip(),
        "component_sku": df[comp_cols[0]].astype(str).str.strip(),
        "quantidade":    pd.to_numeric(br_to_float(df[qtd_cols[0]]), errors="coerce").fillna(0).astype(int)
    })
    # Remove linhas inválidas
    out = out[(out["kit_sku"] != "") & (out["component_sku"] != "")]
    return out.reset_index(drop=True)

# ----------------- Loader principal -----------------
def _carregar_padrao_de_content(content_bytes: bytes) -> Catalogo:
    """
    Lê o XLSX do Google Sheets e retorna Catalogo(catalogo_simples, kits_reais) com:
    - Abas e colunas case-insensitive
    - Preço normalizado para float (sem 'R$' / vírgula)
    - Nenhuma checagem booleana ambígua sobre Series
    """
    if not content_bytes:
        raise RuntimeError("Arquivo de padrão vazio.")

    xls = pd.ExcelFile(io.BytesIO(content_bytes), engine="openpyxl")

    # Escolhe abas por substring, sem depender de maiúsculas/minúsculas
    df_cat_raw  = _pick_sheet_ci(xls, "catalogo", "catalog", "cat")
    df_kits_raw = None
    try:
        df_kits_raw = _pick_sheet_ci(xls, "kits", "kit")
    except Exception:
        # Se não tiver aba de kits, seguimos com vazio — não é erro.
        pass

    # Normalizações (NUNCA usar if sobre Series)
    catalogo_simples = _normalize_catalogo(df_cat_raw)
    kits_reais = _normalize_kits(df_kits_raw) if df_kits_raw is not None else \
                 pd.DataFrame(columns=["kit_sku","component_sku","quantidade"])

    # Retorno no contrato esperado
    return Catalogo(catalogo_simples=catalogo_simples, kits_reais=kits_reais)
# ==== FIM DO PATCH ====
