# auditoria_variacoes_v3.py
import requests, json, sys

BASE = "https://erp.tiny.com.br/public-api/v3"

# Use sua função existente que lê tokens e monta o header Authorization: Bearer
from tiny_api import bearer_headers

def get_json(url, headers, params=None):
    r = requests.get(url, headers=headers, params=params or {}, timeout=30)
    try:
        j = r.json()
    except Exception:
        j = {"raw": r.text}
    return r.status_code, j

def buscar_produtos_por_codigo(conta, codigo):
    """Tenta pegar produto(s) pelo codigo exato."""
    hdr = bearer_headers(conta)
    # Algumas contas aceitam ?codigo=, outras precisam de ?search=
    status, j = get_json(f"{BASE}/produtos", hdr, params={"codigo": codigo})
    if status == 200 and isinstance(j, dict) and "itens" in j and j["itens"]:
        return j["itens"]

    status, j = get_json(f"{BASE}/produtos", hdr, params={"search": codigo})
    if status == 200 and isinstance(j, dict) and "itens" in j and j["itens"]:
        # filtra somente os que realmente têm codigo=codigo
        itens = [x for x in j["itens"] if str(x.get("codigo","")).strip() == codigo]
        return itens if itens else j["itens"]
    return []

def listar_variacoes(conta, id_produto):
    """Lista variações do produto pai."""
    hdr = bearer_headers(conta)
    status, j = get_json(f"{BASE}/produtos/{id_produto}/variacoes", hdr)
    if status == 200 and isinstance(j, dict) and "itens" in j:
        return j["itens"]
    return []

def detalhes_variacao(conta, id_produto, id_variacao):
    """Pega detalhes de uma variação (muitas vezes contém estoque próprio)."""
    hdr = bearer_headers(conta)
    status, j = get_json(f"{BASE}/produtos/{id_produto}/variacoes/{id_variacao}", hdr)
    return status, j

def estoque_produto_geral(conta, id_produto):
    """Estoque agregado do produto (cuidado: soma de variações)."""
    hdr = bearer_headers(conta)
    status, j = get_json(f"{BASE}/estoque/{id_produto}", hdr)
    return status, j

def main():
    if len(sys.argv) < 3:
        print("Uso: python auditoria_variacoes_v3.py <CONTA> <SKU>")
        sys.exit(1)

    conta = sys.argv[1].upper().strip()
    sku   = sys.argv[2].strip()

    print(f"\n=== AUDITORIA | CONTA={conta} | SKU={sku} ===")
    produtos = buscar_produtos_por_codigo(conta, sku)

    if not produtos:
        print("❌ Nenhum produto retornado por esse código. Tentando mostrar possíveis coincidências...")
        # tenta listar algo por search para ver se há duplicidades
        hdr = bearer_headers(conta)
        status, j = get_json(f"{BASE}/produtos", hdr, params={"search": sku})
        if status == 200 and isinstance(j, dict) and "itens" in j:
            print(json.dumps(j["itens"], indent=2, ensure_ascii=False))
        else:
            print("Nada encontrado nem no search.")
        return

    # Pode haver mais de 1, vamos percorrer todos
    for p in produtos:
        pid = p.get("id")
        print(f"\n→ Produto candidato: id={pid} | nome='{p.get('nome')}' | codigo='{p.get('codigo')}'")

        # 1) Variacoes
        vars = listar_variacoes(conta, pid)
        if not vars:
            print("  (Sem variações listadas para este produto)")
        else:
            # printa geral
            print("  ▸ Variações encontradas (id | codigo | nome):")
            for v in vars:
                print(f"    - {v.get('id')} | {v.get('codigo')} | {v.get('nome')}")

            # tenta achar a variação com codigo==SKU
            exata = next((v for v in vars if str(v.get('codigo','')).strip() == sku), None)
            if exata:
                print("\n  ✅ Variação com código EXATO encontrada!")
                vid = exata["id"]
                st, dv = detalhes_variacao(conta, pid, vid)
                print("  Detalhe variação:")
                print(json.dumps(dv, indent=2, ensure_ascii=False))

                # alguns retornam estoque dentro desse detalhe; se não tiver, mostramos estoque do produto
                if "estoque" not in str(dv).lower():
                    st2, je = estoque_produto_geral(conta, pid)
                    print("\n  (Fallback) Estoque AGREGADO do produto:")
                    print(json.dumps(je, indent=2, ensure_ascii=False))
            else:
                print("\n  ⚠️ Nenhuma variação com codigo EXATO igual ao SKU.")
                print("  Verifique se o SKU está salvo no Tiny como código da variação ou do produto pai.")

        # 2) Estoque agregado do produto (para conferência)
        st, je = estoque_produto_geral(conta, pid)
        if st == 200:
            # extrai só o depósito Geral
            dep_geral = None
            for d in je.get("depositos", []):
                if str(d.get("nome","")).strip().lower() == "geral":
                    dep_geral = d; break
            print("\n  🧾 Estoque agregado do produto (v3) - depósito 'Geral':")
            if dep_geral:
                print(json.dumps(dep_geral, indent=2, ensure_ascii=False))
            else:
                print("(Depósito 'Geral' não apareceu na resposta).")
        else:
            print("  (Falha ao ler estoque agregado).")

if __name__ == "__main__":
    main()
