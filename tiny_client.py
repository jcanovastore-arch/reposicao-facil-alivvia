import os
import json
import requests

# Caminho da pasta tokens
TOKEN_DIR = os.path.join(os.path.dirname(__file__), "tokens")

def load_token(empresa):
    """Carrega o token salvo em tokens/tiny_<empresa>.json"""
    path = os.path.join(TOKEN_DIR, f"tiny_{empresa}.json")
    if not os.path.exists(path):
        raise FileNotFoundError(f"Token da empresa {empresa} não encontrado em {path}")
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data["access_token"]

def get_estoque_tiny(empresa, sku):
    """Busca o estoque de um SKU no Tiny"""
    token = load_token(empresa)
    url = "https://api.tiny.com.br/api2/produto.obter.estoque.php"
    params = {
        "token": token,
        "formato": "json",
        "codigo": sku
    }
    try:
        resp = requests.get(url, params=params)
        data = resp.json()
    except Exception as e:
        print(f"❌ Erro ao consultar Tiny ({empresa}): {e}")
        return None

    # Retorno válido?
    if "retorno" not in data or not data["retorno"].get("status") == "OK":
        print(f"⚠️ Erro para {sku} ({empresa}): {data}")
        return None

    produtos = data["retorno"].get("produtos", [])
    if not produtos:
        return None

    produto = produtos[0]["produto"]
    return produto.get("saldo", 0)
