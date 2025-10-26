# testar_v3.py
# Uso: python testar_v3.py ALIVVIA COXAL-HIDRO
#      python testar_v3.py JCA COXAL-HIDRO

import json, sys, os, time
import requests

BASE_V3 = "https://erp.tiny.com.br/public-api/v3"

def load_access_token(conta: str) -> str:
    path = os.path.join("tokens", f"tokens_tiny_{conta}.json")
    with open(path, "r", encoding="utf-8") as f:
        js = json.load(f)
    return js["access_token"]

def h_bearer(token: str) -> dict:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json"
    }

def get_json(url: str, headers: dict, params: dict | None = None):
    r = requests.get(url, headers=headers, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def achar_produto_por_sku(headers: dict, sku: str):
    """
    Tenta achar o produto pelo código/SKU.
    Em geral o Tiny aceita 'codigo' como filtro. Se não retornar,
    tentamos 'pesquisa' como fallback.
    """
    url = f"{BASE_V3}/produtos"

    # 1) tentar por codigo (recomendado)
    for params in ({"codigo": sku}, {"pesquisa": sku}):
        try:
            js = get_json(url, headers, params=params)
        except requests.HTTPError as e:
            # Se 400/404 etc, tenta o próximo
            continue

        # A API v3 retorna paginação; normalmente há campo "items" (ou similar).
        # Vamos procurar em chaves comuns.
        items = None
        for key in ("items", "dados", "data", "produtos", "results"):
            if isinstance(js, dict) and key in js and isinstance(js[key], list):
                items = js[key]
                break
        if not items and isinstance(js, list):
            items = js

        if not items:
            continue

        # Checar por campos típicos
        for p in items:
            codigo = p.get("codigo") or p.get("sku") or p.get("codigoProduto")
            if codigo and codigo.strip().upper() == sku.strip().upper():
                # Retorna estrutura e id
                id_prod = p.get("id") or p.get("idProduto") or p.get("produto_id")
                return id_prod, p

    return None, None

def estoque_por_id(headers: dict, id_prod: str | int):
    url = f"{BASE_V3}/estoque/{id_prod}"
    return get_json(url, headers)

def custos_por_id(headers: dict, id_prod: str | int):
    url = f"{BASE_V3}/produtos/{id_prod}/custos"
    return get_json(url, headers)

def pretty(dic):
    return json.dumps(dic, indent=2, ensure_ascii=False)

def main():
    if len(sys.argv) < 3:
        print("Uso: python testar_v3.py <CONTA: ALIVVIA|JCA> <SKU>")
        sys.exit(1)

    conta = sys.argv[1].strip().upper()
    sku   = sys.argv[2].strip()

    token = load_access_token(conta)
    headers = h_bearer(token)

    print(f"\n=== {conta} | SKU={sku} ===")
    # 1) achar idProduto
    id_prod, prod = achar_produto_por_sku(headers, sku)
    if not id_prod:
        print("❌ Não encontrei produto pelo SKU (tente conferir o código no Tiny).")
        return

    nome = (prod.get("nome") or prod.get("descricao") or "").strip()
    print(f"✔ Produto encontrado: id={id_prod} | nome='{nome}'")

    # 2) estoque
    try:
        est = estoque_por_id(headers, id_prod)
        print("\n📦 ESTOQUE (v3):")
        print(pretty(est))
    except requests.HTTPError as e:
        print(f"\n❌ Erro ao buscar estoque: {e}")

    # 3) custos
    try:
        custos = custos_por_id(headers, id_prod)
        print("\n💰 CUSTOS (v3):")
        print(pretty(custos))
    except requests.HTTPError as e:
        print(f"\n❌ Erro ao buscar custos: {e}")

if __name__ == "__main__":
    main()
