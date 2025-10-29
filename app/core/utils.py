import re
import unicodedata

def norm_header(s: str) -> str:
    s = (s or "").strip()
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii").lower()
    for ch in [" ", "-", "(", ")", "/", "\\", "[", "]", ".", ",", ";", ":"]:
        s = s.replace(ch, "_")
    while "__" in s:
        s = s.replace("__", "_")
    return s.strip("_")

def norm_sku(x: str) -> str:
    if x is None: return ""
    s = unicodedata.normalize("NFKD", str(x)).encode("ascii","ignore").decode("ascii")
    return s.strip().upper()

def br_to_float(x):
    try:
        s = str(x).replace("\u00a0"," ").replace("R$","").replace(" ","").replace(".","").replace(",",".")
        return float(s)
    except:
        try: return float(x)
        except: return 0.0
