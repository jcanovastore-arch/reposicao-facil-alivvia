# teste_sku.py
from tiny_api import get_userinfo, buscar_estoque_por_sku, buscar_custo_medio_por_sku
import json, sys

def _pick(js: dict, *candidatos):
    """Procura valores por possíveis nomes de campo."""
    if isinstance(js, dict):
        for k, v in js.items():
            if k.lower() in candidatos:
                return v
            if isinstance(v, dict):
                achou = _pick(v, *candidatos)
                if achou is not None:
                    return achou
            if isinstance(v, list):
                for it in v:
                    achou = _pick(it, *candidatos)
                    if achou is not None:
                        return achou
    return None

def testar(conta: str, sku: str):
    print(f"\n=== Testando {conta} | SKU={sku} ===")

    try:
        ui = get_userinfo(conta)
        print("✅ Token OK:", ui.get("email"))
    except Exception as e:
        print("⚠️ Erro no token:", e)

    # Estoque
    try:
        js = buscar_estoque_por_sku(conta, sku)
        print("📦 Retorno ESTOQUE:")
        print(json.dumps(js, indent=2, ensure_ascii=False))
        qtd = _pick(js, "estoque", "saldo", "available", "quantity")
        if qtd is not None:
            print(f"➡️ Estoque encontrado: {qtd}")
    except Exception as e:
        print("❌ Erro ao buscar estoque:", e)

    # Custo médio
    try:
        js = buscar_custo_medio_por_sku(conta, sku)
        print("💰 Retorno CUSTO:")
        print(json.dumps(js, indent=2, ensure_ascii=False))
        custo = _pick(js, "custo_medio", "preco_custo", "average_cost", "custo")
        if custo is not None:
            print(f"➡️ Custo médio encontrado: {custo}")
    except Exception as e:
        print("❌ Erro ao buscar custo:", e)

if __name__ == "__main__":
    conta = sys.argv[1] if len(sys.argv) > 1 else "ALIVVIA"
    sku = sys.argv[2] if len(sys.argv) > 2 else "COXAL-HIDRO"
    testar(conta, sku)
