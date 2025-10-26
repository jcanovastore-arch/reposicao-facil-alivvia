# dump_pesquisa.py — lista resultados do produto.pesquisar + variações (códigos e ids)

import sys, requests, json

TOKENS = {
    "ALIVVIA": "b3ca9c3319ac75276c03e097296e15619259cab9029a1e45b781a07553bdb25b",
    "JCA":     "352880e9498ec1a29b81a9f0ea1a946a46415f93b2aa2706634f39064b750dcd",
}

def call(token, url, data):
    data = {**data, "token": token, "formato": "json"}
    r = requests.post(url, data=data, timeout=40)
    return r.status_code, r.json()

def main():
    if len(sys.argv) < 3:
        print("Uso: python dump_pesquisa.py <ALIVVIA|JCA> <termo>"); return
    conta, termo = sys.argv[1].upper(), sys.argv[2]
    token = TOKENS[conta]
    st, js = call(token, "https://api.tiny.com.br/api2/produto.pesquisar.php", {"pesquisa": termo})
    print(f"HTTP {st}")
    ret = js.get("retorno", {}) if isinstance(js, dict) else {}
    itens = ret.get("produtos", []) or []
    print(f"Itens retornados: {len(itens)}\n")
    for i, it in enumerate(itens, 1):
        p = it.get("produto", {}) or {}
        print(f"#{i:02d} id={p.get('id')} | codigo={p.get('codigo')} | nome={p.get('nome')}")
        # variações (se existirem)
        vars = p.get("variacoes") or p.get("variacoes_produto") or []
        if isinstance(vars, dict):  # alguns retornos vêm como {"variacao":[...]}
            vars = vars.get("variacao", [])
        if isinstance(vars, list) and vars:
            for v in vars:
                vv = v.get("variacao", v)  # normaliza
                print(f"    - VAR: id={vv.get('id')} | codigo={vv.get('codigo')} | nome={vv.get('nome')}")
        print()
    # salva arquivo bruto para auditoria
    with open("dump_pesquisa_raw.json", "w", encoding="utf-8") as f:
        json.dump(js, f, ensure_ascii=False, indent=2)
    print("Arquivo bruto salvo: dump_pesquisa_raw.json")

if __name__ == "__main__":
    main()
