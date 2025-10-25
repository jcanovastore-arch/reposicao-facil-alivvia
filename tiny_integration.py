# tiny_integration.py
import time, json, math, os, io
import requests
import pandas as pd
import numpy as np
from unidecode import unidecode

# ===== Normalização e números =====
def _norm(s: str) -> str:
    return unidecode(str(s or "")).strip().lower()

def br_to_float(x):
    if pd.isna(x):
        return np.nan
    if isinstance(x, (int, float, np.integer, np.floating)):
        return float(x)
    s = str(x).strip()
    s = s.replace("\u00a0", " ").replace("R$", "").replace(" ", "")
    has_comma = "," in s
    has_dot   = "." in s
    if has_comma and not has_dot:
        s = s.replace(",", ".")
    elif has_comma and has_dot:
        s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except:
        return np.nan

# ===== Tiny client com retry/backoff =====
class TinyClient:
    def __init__(self, token: str, timeout=30, max_retries=3, backoff_base=0.7):
        self.token = token
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff_base = backoff_base
        self.s = requests.Session()
        self.s.headers.update({"User-Agent": "Alivvia-Reposicao/1.0"})

    def _post(self, url: str, data: dict):
        data = {**data, "token": self.token, "formato": "json"}
        for k in range(self.max_retries):
            try:
                r = self.s.post(url, data=data, timeout=self.timeout)
                if r.status_code == 429 or r.status_code >= 500:
                    time.sleep(self.backoff_base * (2 ** k))
                    continue
                return r
            except requests.RequestException:
                time.sleep(self.backoff_base * (2 ** k))
        return None

    def listar_produtos(self, pagina=1):
        url = "https://api.tiny.com.br/api2/produto.listar.php"
        r = self._post(url, {"pagina": pagina})
        if not r:
            return None, {"erro": "timeout/listar"}
        try:
            payload = r.json()
        except Exception:
            return None, {"erro": f"http {r.status_code}", "raw": r.text[:500]}
        return payload, None

    def obter_estoque_sku(self, sku: str):
        url = "https://api.tiny.com.br/api2/produto.obter.estoque.php"
        r = self._post(url, {"sku": sku})
        if not r:
            return None, {"erro": "timeout/obter_estoque"}
        try:
            payload = r.json()
        except Exception:
            return None, {"erro": f"http {r.status_code}", "raw": r.text[:500]}
        return payload, None

# soma depósito “Geral” (normaliza nome, soma duplicados, ignora desconsiderar=S)
def somar_estoque_geral(depositos: list) -> int:
    total = 0.0
    if not isinstance(depositos, list):
        return 0
    for d in depositos:
        nome = _norm(d.get("nome"))
        if nome != "geral":
            continue
        descons = str(d.get("desconsiderar", "N")).strip().upper()
        if descons == "S":
            continue
        saldo = d.get("saldo", None)
        if saldo in (None, "", "None"):
            saldo = d.get("saldo_disponivel", None)
        if saldo in (None, "", "None"):
            saldo = d.get("qtd", 0)
        val = br_to_float(saldo)
        if pd.isna(val):
            val = 0.0
        total += float(val)
    return int(round(max(total, 0)))

# ====== Leitura robusta com “Modo Confiável” ======
def _auditar_diferencas(dfA, dfB, col="Estoque_Fisico"):
    m = dfA.merge(dfB, on="SKU", how="outer", suffixes=("_A", "_B"))
    m[col+"_A"] = pd.to_numeric(m[col+"_A"], errors="coerce").fillna(0).astype(int)
    m[col+"_B"] = pd.to_numeric(m[col+"_B"], errors="coerce").fillna(0).astype(int)
    m["diff"] = (m[col+"_B"] - m[col+"_A"]).abs()
    m["pct"] = np.where(m[col+"_A"]>0, m["diff"]/m[col+"_A"]*100.0, np.where(m[col+"_B"]>0, 100.0, 0.0))
    return m

def _ler_once(client: TinyClient, skus_alvo: list, sleep_between=0.05):
    linhas = []
    erros = 0
    for sku in skus_alvo:
        payload, err = client.obter_estoque_sku(sku)
        if err:
            erros += 1
            linhas.append({"SKU": sku, "Estoque_Fisico": 0, "Preco": 0.0, "erro": err.get("erro", "?")})
        else:
            dep = payload.get("retorno", {}).get("depositos", [])
            preco = payload.get("retorno", {}).get("produto", {}).get("preco_custo_medio", 0)
            linhas.append({
                "SKU": sku,
                "Estoque_Fisico": somar_estoque_geral(dep),
                "Preco": br_to_float(preco) or 0.0,
                "erro": ""
            })
        if sleep_between:
            time.sleep(sleep_between)
    df = pd.DataFrame(linhas)
    if not df.empty:
        df["SKU"] = df["SKU"].astype(str).str.upper().str.strip()
        df["Estoque_Fisico"] = pd.to_numeric(df["Estoque_Fisico"], errors="coerce").fillna(0).astype(int)
        df["Preco"] = pd.to_numeric(df["Preco"], errors="coerce").fillna(0.0)
    return df, erros

def ler_estoque_confiavel(conta: str, token: str, skus_catalogo: list,
                          delta_pct_outlier=50.0, modo_confiavel=True, ultimo_df=None):
    client = TinyClient(token)
    skus = sorted({str(s).upper().strip() for s in skus_catalogo if str(s).strip()})
    if not skus:
        return pd.DataFrame(columns=["SKU","Estoque_Fisico","Preco"]), {"erro":"catalogo_vazio"}

    dfA, errA = _ler_once(client, skus)
    if not modo_confiavel:
        audit = {"conta": conta, "modo_confiavel": False, "lidos": int(dfA.shape[0]), "erros": int(errA)}
        return dfA[["SKU","Estoque_Fisico","Preco"]], audit

    dfB, errB = _ler_once(client, skus)
    comp = _auditar_diferencas(dfA, dfB, col="Estoque_Fisico")
    outliers = comp.loc[comp["pct"] > float(delta_pct_outlier), "SKU"].dropna().astype(str).tolist()

    dfC = pd.DataFrame(columns=["SKU","Estoque_Fisico","Preco","erro"])
    errC = 0
    if outliers:
        dfC, errC = _ler_once(client, outliers, sleep_between=0.1)

    base = dfB.set_index("SKU")
    if not dfC.empty:
        for sku, row in dfC.set_index("SKU").iterrows():
            base.loc[sku, ["Estoque_Fisico","Preco"]] = [row["Estoque_Fisico"], row["Preco"]]

    a_map = dfA.set_index("SKU")
    for sku in skus:
        if sku not in base.index:
            if sku in a_map.index:
                base.loc[sku, ["Estoque_Fisico","Preco"]] = [int(a_map.loc[sku,"Estoque_Fisico"]), float(a_map.loc[sku,"Preco"])]
            elif ultimo_df is not None and sku in ultimo_df.set_index("SKU").index:
                u = ultimo_df.set_index("SKU")
                base.loc[sku, ["Estoque_Fisico","Preco"]] = [int(u.loc[sku,"Estoque_Fisico"]), float(u.loc[sku,"Preco"])]
            else:
                base.loc[sku, ["Estoque_Fisico","Preco"]] = [0, 0.0]

    base = base.reset_index()
    audit = {
        "conta": conta,
        "modo_confiavel": True,
        "outliers_reconsultados": int(len(outliers)),
        "soma_estoque_final": int(base["Estoque_Fisico"].sum()),
        "ts": time.strftime("%Y-%m-%d %H:%M:%S"),
    }
    return base[["SKU","Estoque_Fisico","Preco"]], audit
