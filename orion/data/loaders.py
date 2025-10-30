"""
ORION.data.loaders — leitura/normalização de dados (CSV/XLSX/Sheets).
(Entrarão aqui: load_any_table_from_bytes, normalize_cols, mapear_tipo, mapear_colunas…)
"""

from __future__ import annotations
import io
import pandas as pd
from . import __init__  # noqa: F401
from orion.core.utils import norm_header, norm_sku  # quando migrarmos, já está pronto

def placeholder():
    """Mantido vazio neste passo. Implementaremos no Passo 2/3."""
    return True
