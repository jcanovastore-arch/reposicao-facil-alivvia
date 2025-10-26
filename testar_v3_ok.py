# testar_v3_ok.py
# ----------------
# Teste de SKU no Tiny v3 com suporte a VARIAÇÕES e filtro do depósito "Geral".

import json
import os
import sys
import requests

API_BASE = "https://erp.tiny.com.br/public-api/v3"

def load_token(conta: str) -> str:
    """
    Lê o access_token salvo em tokens/tokens_tiny_{CONTA}.json
    """
    caminho = os.path.join("tokens", f"tokens_tiny_{conta}.json")
    if not os.path.exists(caminho):
        raise FileNotFoundError(f"❌ Token não encontrado: {caminho}")
    with open(caminho, "r", encoding="utf-8") as f:
        js = json.load(f)
    return js["access_token"]

def h(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}

def get_json(url, token, params=None):
    r = requests.get(url, headers=h(token), params=params or {})
    if r.status_code == 204:
        return {}
    if r.status_code >= 400:
        raise requests.HTTPError(f"{r.status_code} {r.text}")
    return r.json()

def resolve_produto_id_por_sku(token: str, sku: str) -> dict:
    """
    Resolve o ID correto do produto/variação para um given SKU.
    - Primeiro busca /produtos?codigo=SKU
    - Se for pai com variações, busca /produtos/{idPai}/variacoes,
      encontra a variação cujo 'codigo' == SKU e usa o ID da variação.
    Retorna um dict com:
        {
          "id": <id do produto/variação>,
          "produto": <obj retornado do /produtos?codigo>,
          "variacao": <obj variação selecionada ou None>
        }
    """
    # 1) Tenta encontrar o produto por código direto
    data = get_json(f"{API_BASE}/produtos", token, params={"codigo": sku})
    itens = data.get("itens", [])
    if not itens:
        return {"id": None, "produto": None, "variacao": None}

    produto = itens[0]     # Pode ser pai ou a variação diretamente
    id_produto = produto["id"]

    # 2) Verifica se há variações
    try:
        var_data = get_json(f"{API_BASE}/produtos/{id_produto}/variacoes", token)
        variacoes = var_data.get("itens", [])
    except requests.HTTPError:
        variacoes = []

    variacao_escolhida = None
    if variacoes:
        # caso seja um pai, acha a variação pelo código exato
        for v in variacoes:
            if v.get("codigo") == sku:
                variacao_escolhida = v
                id_produto = v["id"]
                break

    return {"id": id_produto, "produto": produto, "variacao": variacao_escolhida}

def get_estoque_geral_v3(token: str, id_produto: int) -> dict:
    """
    Busca /estoque/{idProduto} e retorna somente os valores do depósito 'Geral'
    onde desconsiderar == false.
    """
    est = get_json(f"{API_BASE}/estoque/{id_produto}", token)
    depositos = est.get("depositos", []) or []

    geral = next(
        (d for d in depositos if d.get("nome") == "Geral" and not d.get("desconsiderar", False)),
        None
    )

    return {
        "id": est.get("id"),
        "nome": est.get("nome"),
        "codigo": est.get("codigo"),
        "unidade": est.get("unidade"),
        "saldo_geral": geral.get("saldo", 0) if geral else 0,
        "reservado_geral": geral.get("reservado", 0) if geral else 0,
        "disponivel_geral": geral.get("disponivel", 0) if geral else 0,
        "depositos": depositos,  # mantém para debug se quiser
    }

def get_custos_v3(token: str, id_produto: int) -> dict:
    """
    Busca custos da API v3: /produtos/{idProduto}/custos
    """
    # Você pode ajustar limit/offset conforme sua necessidade
    return get_json(f"{API_BASE}/produtos/{id_produto}/custos", token, params={"limit": 100, "offset": 0})

def main():
    if len(sys.argv) < 3:
        print("Uso: python testar_v3_ok.py <CONTA> <SKU>")
        print("Exemplo: python testar_v3_ok.py JCA LUVA-NEOPRENE-PRETA-G")
        sys.exit(1)

    conta = sys.argv[1].strip().upper()
    sku = " ".join(sys.argv[2:]).strip()

    print(f"\n=== {conta} | SKU={sku} ===")

    try:
        token = load_token(conta)
    except Exception as e:
        print(f"❌ {e}")
        sys.exit(1)

    # Resolve ID correto (variação quando necessário)
    try:
        res = resolve_produto_id_por_sku(token, sku)
    except requests.HTTPError as e:
        print(f"❌ Erro ao resolver produto por SKU: {e}")
        sys.exit(1)

    if not res["id"]:
        print("❌ Não encontrei produto pelo SKU. Confira o 'código' no Tiny (v3).")
        sys.exit(1)

    id_resolvido = res["id"]
    produto = res["produto"]
    variacao = res["variacao"]

    # Prints de debug úteis
    if variacao:
        print(f"✔ Produto PAI: id={produto['id']} | nome='{produto.get('nome')}'")
        print(f"✔ Variação encontrada: id={variacao['id']} | codigo='{variacao.get('codigo')}' | nome='{variacao.get('nome')}'")
    else:
        print(f"✔ Produto encontrado: id={produto['id']} | nome='{produto.get('nome')}' | codigo='{produto.get('codigo')}'")

    # ESTOQUE (apenas depósito Geral)
    try:
        est_geral = get_estoque_geral_v3(token, id_resolvido)
        print("\n📦 ESTOQUE - Depósito 'Geral' (v3):")
        print(json.dumps(est_geral, indent=2, ensure_ascii=False))
    except requests.HTTPError as e:
        print(f"❌ Erro ao buscar estoque: {e}")

    # CUSTOS
    try:
        custos = get_custos_v3(token, id_resolvido)
        print("\n💰 CUSTOS (v3):")
        print(json.dumps(custos, indent=2, ensure_ascii=False))
    except requests.HTTPError as e:
        print(f"❌ Erro ao buscar custos: {e}")

if __name__ == "__main__":
    main()
