import requests
import json
import sys
from datetime import datetime
import os

# ========================
# CONFIGURAÇÕES DAS CONTAS
# ========================

CONTAS = {
    "ALIVVIA": {
        "token": "b3ca9c3319ac75276c03e097296e15619259cab9029a1e45b781a07553bdb25b"
    },
    "JCA": {
        "token": "352880e9498ec1a29b81a9f0ea1a946a46415f93b2aa2706634f39064b750dcd"
    }
}

# ===============
# FUNÇÃO PRINCIPAL
# ===============

def buscar_produto(conta_nome, sku):
    if conta_nome not in CONTAS:
        print(f"❌ Conta '{conta_nome}' não encontrada nas configurações.")
        return

    token = CONTAS[conta_nome]["token"]
    url = "https://api.tiny.com.br/api2/produto.obter.php"
    params = {
        "token": token,
        "formato": "json",
        "codigo": sku
    }

    print(f"🔎 Pesquisando SKU: {sku}")
    print(f"Conta: {conta_nome}")
    print(f"Endpoint: {url}")

    try:
        resp = requests.post(url, data=params)
    except Exception as e:
        print(f"❌ Erro de conexão: {e}")
        return

    print(f"Status HTTP: {resp.status_code}")

    # cria pasta de logs
    os.makedirs("logs", exist_ok=True)
    log_path = f"logs/{conta_nome}_{sku}_tiny_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

    try:
        data = resp.json()
    except Exception:
        print("❌ Erro ao interpretar JSON da API.")
        print("Resposta bruta (HTML):")
        print(resp.text[:500])
        return

    with open(log_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    retorno = data.get("retorno", {})
    if retorno.get("status") != "OK":
        print(f"❌ Erro retornado pelo Tiny:")
        print(json.dumps(retorno.get("erros", []), indent=2, ensure_ascii=False))
        return

    produto = retorno.get("produto", {})
    nome = produto.get("nome", "N/A")
    codigo = produto.get("codigo", "N/A")
    preco = produto.get("preco_custo_medio", "N/A")
    estoque = produto.get("saldo_fisico_total", "N/A")

    print("\n=== RESULTADO ===")
    print(f"Nome: {nome}")
    print(f"SKU: {codigo}")
    print(f"Preço médio: {preco}")
    print(f"Estoque físico total: {estoque}")
    print(f"Log salvo em: {log_path}")


# ====================
# EXECUÇÃO VIA TERMINAL
# ====================

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Uso correto: python teste_api_tiny.py <CONTA> <SKU>")
        print("Exemplo: python teste_api_tiny.py ALIVVIA COXAL-HIDRO")
    else:
        conta = sys.argv[1].upper()
        sku = sys.argv[2].strip()
        buscar_produto(conta, sku)
