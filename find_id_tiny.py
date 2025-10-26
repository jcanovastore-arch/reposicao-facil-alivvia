# find_id_tiny.py — varre produto.listar (paginado) e encontra ID por codigo (inclui VARIAÇÕES)
import sys, time, requests

TOKENS = {
    "ALIVVIA": "b3ca9c3319ac75276c03e097296e15619259cab9029a1e45b781a07553bdb25b",
    "JCA":     "352880e9498ec1a29b81a9f0ea1a946a46415f93b2aa2706634f39064b750dcd",
}

S = requests.Session()
S.headers.update({"User-Agent": "Tiny-FindID/1.0"})

def tiny_post(url, data, token):
    data = {**data, "token": token, "formato": "json"}
    r = S.post(url, data=data, timeout=60)
    try:
        return r.status_code, r.json()
    except Exception:
        return r.status_code, {"retorno": {"status": "Erro", "erros": [{"erro": r.text[:300]}]}}

def normalize(x): 
    return (x or "").strip()

def find_id(conta, codigo_alvo, max_pag=200):
    token = TOKENS[conta]
    codigo_alvo = normalize(codigo_alvo)
    print(f"[{conta}] procurando codigo='{codigo_alvo}' em até {max_pag} páginas…")

    achados = []
    for pagina in range(1, max_pag + 1):
        st, js = tiny_post("https://api.tiny.com.br/api2/produto.listar.php",
                           {"pagina": pagina}, token)
        itens = (js.get("retorno", {}) or {}).get("produtos", []) or []
        if not itens:
            print(f"  página {pagina}: vazia (HTTP {st}). Encerrando.")
            break

        for it in itens:
            p = it.get("produto", {}) or {}
            pid = normalize(p.get("id"))
            pcod = normalize(p.get("codigo"))
            pnom = normalize(p.get("nome"))

            if pcod.upper() == codigo_alvo.upper():
                achados.append(("PRODUTO", pid, pcod, pnom))

            # variações (formatos diferentes)
            vars_obj = p.get("variacoes") or p.get("variacoes_produto") or {}
            variacoes = []
            if isinstance(vars_obj, dict):
                variacoes = vars_obj.get("variacao", []) or vars_obj.get("variacoes", []) or []
            elif isinstance(vars_obj, list):
                variacoes = vars_obj

            for v in variacoes:
                vv = v.get("variacao", v)
                vid = normalize(vv.get("id"))
                vcod = normalize(vv.get("codigo"))
                vnom = normalize(vv.get("nome"))
                if vcod.upper() == codigo_alvo.upper():
                    achados.append(("VARIACAO", vid, vcod, vnom or pnom))

        print(f"  página {pagina}: {len(itens)} itens varridos.")
        time.sleep(0.6)

    if not achados:
        print("NADA ENCONTRADO para esse código.")
    else:
        print("\n=== ACHADOS ===")
        for t, _id, cod, nome in achados:
            print(f"- tipo={t} | id={_id} | codigo={cod} | nome={nome}")
    return achados

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Uso: python find_id_tiny.py <ALIVVIA|JCA> <CODIGO>")
        sys.exit(0)
    conta, codigo = sys.argv[1].upper(), " ".join(sys.argv[2:])
    find_id(conta, codigo)
