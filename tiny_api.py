# tiny_api.py
from __future__ import annotations
import json, os
import requests

# === CONFIGURAÇÃO DOS TOKENS =====================================
# Pasta onde estão os arquivos de token:
TOKENS_DIR = os.path.join(os.path.dirname(__file__), "tokens")

# === FUNÇÕES DE TOKEN ============================================

def _token_path(conta: str) -> str:
    """Mapeia o nome lógico da conta para o arquivo de token."""
    nome = conta.strip().upper()
    if nome == "JCA":
        fname = "tokens_tiny_JCA.json"
    elif nome == "ALIVVIA":
        fname = "tokens_tiny_ALIVVIA.json"
    else:
        raise ValueError("Conta inválida. Use 'JCA' ou 'ALIVVIA'.")
    return os.path.join(TOKENS_DIR, fname)

def load_access_token(conta: str) -> str:
    """Lê o access_token salvo para a conta."""
    path = _token_path(conta)
    with open(path, "r", encoding="utf-8") as f:
        js = json.load(f)
    return js["access_token"]

def bearer_headers(conta: str) -> dict:
    """Monta os headers com Authorization: Bearer ..."""
    token = load_access_token(conta)
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }

# === USERINFO (teste de token) ===================================

USERINFO_URL = "https://accounts.tiny.com.br/realms/tiny/protocol/openid-connect/userinfo"

def get_userinfo(conta: str) -> dict:
    """Valida o token chamando o userinfo (funciona sempre)."""
    h = bearer_headers(conta)
    r = requests.post(USERINFO_URL, headers=h, timeout=30)
    r.raise_for_status()
    return r.json()

# === LEITURA GERAL (API Platform OAuth) ==========================

BASE_API = "https://api.tiny.com.br"

def platform_get(conta: str, path: str, params: dict | None = None) -> dict:
    """
    Faz GET em qualquer endpoint da API nova (Platform) com OAuth Bearer.
    Exemplo: path="platform/products"
    """
    url = f"{BASE_API}/{path.lstrip('/')}"
    h = bearer_headers(conta)
    r = requests.get(url, headers=h, params=params or {}, timeout=40)
    r.raise_for_status()
    return r.json()

# === CAMINHOS PADRÃO (ajuste depois conforme a doc da Tiny) ======
PATH_ESTOQUE = "platform/inventory/items"  # Troque pelo caminho real
PATH_CUSTO   = "platform/products"         # Troque pelo caminho real

def buscar_estoque_por_sku(conta: str, sku: str) -> dict:
    """
    Consulta o estoque por SKU.
    Ajuste o nome do parâmetro conforme a doc (ex: 'product_sku' ou 'sku').
    """
    return platform_get(conta, PATH_ESTOQUE, params={"sku": sku})

def buscar_custo_medio_por_sku(conta: str, sku: str) -> dict:
    """
    Consulta o produto para obter o custo médio.
    Ajuste PATH_CUSTO e o nome do parâmetro conforme a doc.
    """
    return platform_get(conta, PATH_CUSTO, params={"sku": sku})
