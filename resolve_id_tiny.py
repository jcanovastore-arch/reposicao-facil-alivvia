# resolve_id_tiny.py — acha o ID (produto ou variação) dado um CODIGO/SKU.
# Tenta: produto.obter(codigo) -> produto.pesquisar(pesquisa) -> produto.listar(pesquisa)
import sys, time, requests

TOKENS = {
    "ALIVVIA": "b3ca9c3319ac75276c03e097296e15619259cab9029a1e45b781a07553bdb25b",
    "JCA":     "352880e9498ec1a29b81a9f0ea1a946a46415f93b2aa2706634f39064b750dcd",
}

S = requests.Session()
S.headers.update({"User-Agent": "Tiny-ResolveID/1.0"})

def post_json(url, data, token, timeout=50):
    data = {**data, "token": token, "formato": "json"}
    r = S.post(url, data=data, timeout=timeout)
    try:
        return r.status_code, r.json()
    except Exception:
        return r.status_code, {"retorno": {"status": "Erro", "erros": [{"erro": r.text[:400]}]}}

def norm(x): return (x or "").strip()
def eq(a,b): return norm(a).upper() == norm(b).upper()

def pick_variation_id(product_obj, codigo_alvo):
    # Tenta achar variação cujo codigo == codigo_alvo
    vars_obj = product_obj.get("variacoes") or product_obj.get("variacoes_produto") or {}
    variacoes = []
    if isinstance(vars_obj, dict):
        variacoes = vars_obj.get("variacao", []) or vars_obj.get("variacoes", []) or []
    elif isinstance(vars_obj, list):
        variacoes = vars_obj
    for v in variacoes:
        vv = v.get("variacao", v)
        if eq(vv.get("codigo"), codigo_alvo):
            return norm(vv.get("id"))
    return None

def via_obter(token, codigo):
    st, js = post_json("https://api.tiny.com.br/api2/produto.obter.php",
                       {"codigo": codigo}, token)
    ret = js.get("retorno", {}) if isinstance(js, dict) else {}
    if ret.get("status") != "OK":
        return None, ("via_obter", st, ret.get("erros"))
    prod = ret.get("produto", {}) or {}
    # Se o próprio produto tem o mesmo codigo, usar id do produto;
    # senão tentar variação com esse codigo
    if eq(prod.get("codigo"), codigo) and prod.get("id"):
        return norm(prod.get("id")), None
    var_id = pick_variation_id(prod, codigo)
    if var_id:
        return var_id, None
    return None, ("via_obter_sem_match", st, None)

def via_pesquisar(token, codigo):
    st, js = post_json("https://api.tiny.com.br/api2/produto.pesquisar.php",
                       {"pesquisa": codigo}, token)
    ret = js.get("retorno", {}) if isinstance(js, dict) else {}
    prods = ret.get("produtos", []) or []
    if not prods:
        return None, ("via_pesquisar_vazio", st, ret.get("erros"))
    # procura match exato no produto
    for it in prods:
        p = it.get("produto", {}) or {}
        if eq(p.get("codigo"), codigo) and p.get("id"):
            return norm(p.get("id")), None
        var_id = pick_variation_id(p, codigo)
        if var_id:
            return var_id, None
    return None, ("via_pesquisar_sem_match", st, None)

def via_listar(token, codigo):
    st, js = post_json("https://api.tiny.com.br/api2/produto.listar.php",
                       {"pagina": 1, "pesquisa": codigo}, token)
    ret = js.get("retorno", {}) if isinstance(js, dict) else {}
    prods = ret.get("produtos", []) or []
    if not prods:
        return None, ("via_listar_vazio", st, ret.get("erros"))
    for it in prods:
        p = it.get("produto", {}) or {}
        if eq(p.get("codigo"), codigo) and p.get("id"):
            return norm(p.get("id")), None
        var_id = pick_variation_id(p, codigo)
        if var_id:
            return var_id, None
    return None, ("via_listar_sem_match", st, None)

def resolve_id(conta, codigo):
    token = TOKENS.get(conta)
    if not token: return None, "conta_sem_token"

    codigo = norm(codigo)
    trail = []

    # 1) obter por codigo
    _id, err = via_obter(token, codigo)
    if _id: return _id, None
    trail.append(err)

    # 2) pesquisar
    time.sleep(0.6)
    _id, err = via_pesquisar(token, codigo)
    if _id: return _id, None
    trail.append(err)

    # 3) listar com pesquisa
    time.sleep(0.6)
    _id, err = via_listar(token, codigo)
    if _id: return _id, None
    trail.append(err)

    return None, trail

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Uso: python resolve_id_tiny.py <ALIVVIA|JCA> <CODIGO>")
        sys.exit(0)
    conta, cod = sys.argv[1].upper(), " ".join(sys.argv[2:])
    _id, info = resolve_id(conta, cod)
    if _id:
        print(f"✅ ID encontrado: {conta} | codigo={cod} | id={_id}")
    else:
        print("❌ Não consegui resolver o ID.")
        print("Rastro de tentativas:")
        for item in (info or []):
            print(" -", item)
