import argparse, json, os, time, sys, re
from typing import Optional, Tuple, Dict, Any, List
import requests

TINY_OIDC = "https://accounts.tiny.com.br/realms/tiny/protocol/openid-connect/token"
TINY_API  = "https://erp.tiny.com.br/public-api/v3"

# -------------------------
# Credenciais & Bearer
# -------------------------
def load_secrets(empresa: str) -> Dict[str, str]:
    """
    Busca credenciais em (ordem):
      1) Variáveis de ambiente:
         TINY_<EMP>_CLIENT_ID / _CLIENT_SECRET / _REFRESH_TOKEN
      2) Arquivo tiny_secrets.json no diretório atual:
         { "ALIVVIA": {"client_id":"...","client_secret":"...","refresh_token":"..."},
           "JCA":     {"client_id":"...","client_secret":"...","refresh_token":"..."} }
    """
    emp = empresa.upper()
    env = {
        "client_id":     os.getenv(f"TINY_{emp}_CLIENT_ID"),
        "client_secret": os.getenv(f"TINY_{emp}_CLIENT_SECRET"),
        "refresh_token": os.getenv(f"TINY_{emp}_REFRESH_TOKEN"),
    }
    if all(env.values()):
        return env

    if os.path.exists("tiny_secrets.json"):
        with open("tiny_secrets.json", "r", encoding="utf-8") as f:
            data = json.load(f)
        if emp in data:
            need = data[emp]
            if all(k in need and need[k] for k in ("client_id","client_secret","refresh_token")):
                return need

    raise RuntimeError("tiny_secrets.json não encontrado e ENV de %s ausentes." % emp)

def token_cache_path(empresa: str) -> str:
    os.makedirs("tokens", exist_ok=True)
    return os.path.join("tokens", f"tiny_{empresa.upper()}.json")

def save_access_token(empresa: str, access_token: str, expires_in: Optional[int]=None):
    data = {"access_token": access_token}
    if expires_in:
        data["expires_in"] = expires_in
        data["saved_at"] = int(time.time())
    with open(token_cache_path(empresa), "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def load_access_token(empresa: str) -> Optional[str]:
    p = token_cache_path(empresa)
    if not os.path.exists(p):
        return None
    try:
        with open(p, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data.get("access_token")
    except Exception:
        return None

def refresh_bearer(empresa: str) -> str:
    secrets = load_secrets(empresa)
    payload = {
        "grant_type": "refresh_token",
        "client_id": secrets["client_id"],
        "client_secret": secrets["client_secret"],
        "refresh_token": secrets["refresh_token"],
    }
    r = requests.post(TINY_OIDC, data=payload, timeout=30)
    if not r.ok:
        raise RuntimeError(f"Falha ao renovar token ({empresa}): {r.status_code} {r.text[:200]}")
    data = r.json()
    access_token = data.get("access_token")
    if not access_token:
        raise RuntimeError("Tiny não retornou access_token.")
    save_access_token(empresa, access_token, data.get("expires_in"))
    return access_token

def get_bearer(empresa: str) -> str:
    tok = load_access_token(empresa)
    if tok:
        return tok
    return refresh_bearer(empresa)

# -------------------------
# HTTP helper
# -------------------------
def tiny_req(token: str, method: str, path: str, params=None) -> Dict[str,Any]:
    url = f"{TINY_API}/{path.lstrip('/')}"
    headers = {"Authorization": f"Bearer {token}"}
    for attempt in range(3):
        r = requests.request(method, url, params=params, headers=headers, timeout=30)
        if r.status_code in (429,500,502,503,504):
            time.sleep(1.1*(attempt+1))
            continue
        if r.status_code == 401:
            raise RuntimeError("401 Unauthorized (access_token inválido/expirado).")
        if not r.ok:
            raise RuntimeError(f"HTTP {r.status_code} em {path}: {r.text[:300]}")
        try:
            return r.json()
        except Exception:
            raise RuntimeError(f"Resposta não-JSON em {path}: {r.text[:300]}")
    raise RuntimeError(f"Falha repetida em {path}")

# -------------------------
# Util: busca de campos
# -------------------------
def br_to_float(x) -> Optional[float]:
    if x in (None, "", []):
        return None
    s = str(x).strip()
    s = s.replace("\u00a0", " ").replace("R$", "").replace(" ", "").replace(".", "").replace(",", ".")
    try:
        return float(s)
    except:
        try:
            return float(x)
        except:
            return None

def node_code(n: Any) -> Optional[str]:
    if isinstance(n, dict):
        for k in ("codigo","sku","codigo_sku","codigo_item","codigo_interno"):
            v = n.get(k)
            if v not in (None,""):
                return str(v).strip().upper()
    return None

def node_id(n: Any) -> Optional[int]:
    if isinstance(n, dict):
        for k in ("id","idVariacao","id_variacao","idProduto"):
            v = n.get(k)
            if v not in (None,"",[]):
                try: return int(v)
                except: pass
    return None

PRICE_PRIORITY = [
    "precoCustoMedio","preco_custo_medio","custoMedio","custo_medio",
    "precoCusto","preco_custo","precoCompra","preco_compra",
    "precoMedio","preco_medio",
    "preco","precoVenda","preco_venda",
    "precoPromocional","preco_promocional"
]

def search_price(node: Any) -> Optional[float]:
    if not isinstance(node, dict): return None
    spaces = [node]
    for k in ("precos","data"):
        if k in node and isinstance(node[k], dict):
            spaces.append(node[k])
    for space in spaces:
        for key in PRICE_PRIORITY:
            if key in space and space[key] not in (None,"",[]):
                v = br_to_float(space[key])
                if v is not None:
                    return v
    return None

def scan_for_sku(node: Any, skuU: str, out: Dict[str,Any]):
    if isinstance(node, dict):
        if node_code(node) == skuU:
            if "id" not in out or out["id"] is None:
                out["id"] = node_id(node)
            out["ean"] = out.get("ean") or node.get("ean") or node.get("gtin") or ""
            out["price_node"] = node
        for v in node.values():
            scan_for_sku(v, skuU, out)
    elif isinstance(node, list):
        for e in node:
            scan_for_sku(e, skuU, out)

# -------------------------
# Lógica: variação + estoque
# -------------------------
def resolve_variacao_por_sku(token: str, sku: str) -> Tuple[Optional[int], Optional[int], Optional[str], Optional[float]]:
    skuU = (sku or "").strip().upper()
    if not skuU:
        return None, None, None, None

    # 1) busca pai pelo código
    data = tiny_req(token, "GET", "/produtos", params={"codigo": skuU})
    itens = data.get("itens") or data.get("items") or data.get("data") or []
    if not itens: return None, None, None, None
    pai_id = itens[0].get("id")
    if not pai_id: return None, None, None, None
    pai_id = int(pai_id)

    # 2) varre o detalhe
    det = tiny_req(token, "GET", f"/produtos/{pai_id}")
    found = {}
    scan_for_sku(det, skuU, found)
    if found.get("id"):
        preco = search_price(found.get("price_node", {}))
        return pai_id, int(found["id"]), (found.get("ean") or ""), preco

    # 3) fallback em /variacoes
    try:
        v = tiny_req(token, "GET", f"/produtos/{pai_id}/variacoes")
        for key in ("variacoes","items","data","dados"):
            lst = v.get(key)
            if not lst: continue
            for n in lst:
                if node_code(n) == skuU:
                    vid = node_id(n)
                    ean = n.get("ean") or n.get("gtin") or ""
                    preco = search_price(n)
                    return pai_id, (int(vid) if vid else None), str(ean), preco
    except Exception:
        pass

    return pai_id, None, None, None

def get_estoque_geral(token: str, prod_or_var_id: int) -> Dict[str,Any]:
    data = tiny_req(token, "GET", f"/estoque/{prod_or_var_id}")
    depositos = data.get("depositos") or data.get("data", {}).get("depositos") or data.get("data", {}).get("deposito") or []
    if not isinstance(depositos, list):
        depositos = [depositos] if depositos else []
    pref = None
    for d in depositos:
        if (d.get("nome") or "").strip().lower() == "geral":
            pref = d; break
    if not pref and depositos:
        pref = next((d for d in depositos if d.get("desconsiderar") is False), depositos[0])
    if not pref: return {}
    saldo = int(pref.get("saldo") or 0)
    reservado = int(pref.get("reservado") or 0)
    disponivel = int(pref.get("disponivel") or (saldo - reservado))
    return {"deposito": pref.get("nome") or "", "saldo": saldo, "reservado": reservado, "disponivel": disponivel}

# -------------------------
# Execução
# -------------------------
def process(empresa: str, skus: List[str]) -> List[Dict[str,Any]]:
    token = get_bearer(empresa)
    out = []
    for sku in skus:
        try:
            pai_id, var_id, ean, preco = resolve_variacao_por_sku(token, sku)
            if not pai_id:
                out.append({"empresa":empresa,"sku":sku,"status":"SKU não encontrado","variacao_id":None,"preco":0.0,"estoque":{}})
                continue
            if not var_id:
                out.append({"empresa":empresa,"sku":sku,"status":"PAI (variação não encontrada)","variacao_id":None,"preco":float(preco or 0.0),"estoque":{}})
                continue
            est = get_estoque_geral(token, var_id)
            out.append({
                "empresa": empresa,
                "sku": sku,
                "pai_id": pai_id,
                "variacao_id": var_id,
                "ean": ean or "",
                "preco": float(preco or 0.0),
                "estoque": est,
                "status": "OK"
            })
        except Exception as e:
            out.append({"empresa":empresa,"sku":sku,"status":f"ERRO: {e}","variacao_id":None,"preco":0.0,"estoque":{}})
    return out

def main():
    ap = argparse.ArgumentParser(description="Teste Tiny v3 - variações + custo + estoque(Geral)")
    ap.add_argument("--empresa", required=True, choices=["ALIVVIA","JCA"])
    ap.add_argument("--skus", required=True, help='Lista de SKUs separados por vírgula. Ex.: "LUVA-NEOPRENE-PRETA-G, OUTRO-SKU"')
    args = ap.parse_args()

    skus = [s.strip() for s in args.skus.split(",") if s.strip()]
    try:
        rows = process(args.empresa, skus)
        print(json.dumps(rows, ensure_ascii=False, indent=2))
    except RuntimeError as e:
        # Se access_token expirou, tenta 1 refresh e refaz:
        if "401 Unauthorized" in str(e):
            refresh_bearer(args.empresa)
            rows = process(args.empresa, skus)
            print(json.dumps(rows, ensure_ascii=False, indent=2))
        else:
            raise

if __name__ == "__main__":
    main()
