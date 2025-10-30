"""
ORION.core.utils — funções utilitárias genéricas.
(Serão migradas do reposicao_facil.py aos poucos, por passos.)
"""

from __future__ import annotations
import re
import unicodedata

def norm_header(s: str) -> str:
    if s is None: return ""
    s = (s or "").strip()
    s = unicodedata.normalize("NFKD", s).encode("ASCII", "ignore").decode("ASCII")
    s = s.lower()
    for ch in [" ", "-", "(", ")", "/", "\\", "[", "]", ".", ",", ";", ":"]:
        s = s.replace(ch, "_")
    while "__" in s:
        s = s.replace("__", "_")
    return s.strip("_")

def norm_sku(x: str) -> str:
    if x is None: return ""
    s = unicodedata.normalize("NFKD", str(x)).encode("ASCII","ignore").decode("ASCII")
    return s.strip().upper()
