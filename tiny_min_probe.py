# tiny_min_probe.py
import os, sys, json, time, requests

TOKENS = {
    "ALIVVIA": "b3ca9c3319ac75276c03e097296e15619259cab9029a1e45b781a07553bdb25b",
    "JCA":     "352880e9498ec1a29b81a9f0ea1a946a46415f93b2aa2706634f39064b750dcd",
}

S = requests.Session()
S.headers.update({"User-Agent": "Alivvia-MinProbe/1.0"})

def call(url, data, token):
    data = {**data, "token": token, "formato": "json"}
    r = S.post(url, data=data, timeout=15)
    try:
        js = r.json()
    except Exception:
        js = {"_raw": r.text[:800]}
    return r.status_code, js

def dump(name, status, js):
    print(f"\n== {name} ==")
    print("HTTP:", status)
    ret = (js or {}).get("retorno")
    if ret:
        print("retorno.status:", ret.get("status"))
        if ret.get("erros"):
            print("erros:", ret.get("erros"))
        if ret.get("produtos") is not None:
            print("qtd produtos na página:", len(ret.get("produtos") or []))
    else:
        print("(sem chave 'retorno')")
    if "_raw" in (js or {}):
        print("RAW (inicio):", js["_raw"][:200].replace("\n"," "))

def main(conta):
    token = TOKENS.get(conta)
    if not token:
        print("Conta inválida.")
        sys.exit(1)

    os.makedirs("logs", exist_ok=True)

    # 1) produto.listar página 1
    st1, js1 = call("https://api.tiny.com.br/api2/produto.listar.php",
                    {"pagina": 1}, token)
    dump("listar.pagina1", st1, js1)

    # 2) produto.pesquisar com termo genérico (2 letras)
    st2, js2 = call("https://api.tiny.com.br/api2/produto.pesquisar.php",
                    {"pesquisa": "AA"}, token)
    dump("pesquisar('AA')", st2, js2)

    # 3) produto.obter.estoque EXIGE id; aqui forço id=1 só para ver a mensagem de erro
    st3, js3 = call("https://api.tiny.com.br/api2/produto.obter.estoque.php",
                    {"id": 1}, token)
    dump("obter.estoque(id=1)", st3, js3)

if __name__ == "__main__":
    conta = sys.argv[1] if len(sys.argv) > 1 else "ALIVVIA"
    main(conta)
