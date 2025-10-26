# tiny_probe_quick.py
import os, sys, time, json, argparse, requests

TOKENS = {
    "ALIVVIA": "b3ca9c3319ac75276c03e097296e15619259cab9029a1e45b781a07553bdb25b",
    "JCA":     "352880e9498ec1a29b81a9f0ea1a946a46415f93b2aa2706634f39064b750dcd",
}

LOG_DIR = "logs"
UA = "Alivvia-Tiny-ProbeQuick/1.0"

S = requests.Session()
S.headers.update({"User-Agent": UA})

def ensure_logs():
    os.makedirs(LOG_DIR, exist_ok=True)

def save_json(tag, conta, key, payload):
    ensure_logs()
    ts = time.strftime("%Y%m%d_%H%M%S")
    path = f"{LOG_DIR}/{conta}_{key}_{tag}_{ts}.json"
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, ensure_ascii=False)
        return path
    except:
        return None

def post_json(url, data, token, conta, key, label, timeout=12, tries=2, backoff=0.7):
    data = {**data, "token": token, "formato": "json"}
    last_err = None
    for i in range(1, tries+1):
        print(f"  → [{label}] POST {url} (tentativa {i}/{tries})", flush=True)
        try:
            r = S.post(url, data=data, timeout=timeout)
            print(f"    … HTTP {r.status_code}", flush=True)
            try:
                js = r.json()
            except Exception as ejson:
                txt = r.text[:1000]
                js = {"_parse_error": str(ejson), "_text": txt}
            save_json(label, conta, key, js)
            return r.status_code, js
        except Exception as e:
            last_err = str(e)
            print(f"    ✖ erro: {last_err}", flush=True)
            time.sleep(backoff * i)
    return 0, {"retorno": {"status": "Erro", "erros": [{"erro": f"Falha HTTP: {last_err}"}]}}

def norm(x): return (x or "").strip()
def equal(a,b): return norm(a).upper() == norm(b).upper()

def iter_variacoes(prod):
    vobj = (prod or {}).get("variacoes") or (prod or {}).get("variacoes_produto") or {}
    items = []
    if isinstance(vobj, dict):
        items = vobj.get("variacao") or vobj.get("variacoes") or []
    elif isinstance(vobj, list):
        items = vobj
    out = []
    for it in items:
        if isinstance(it, dict) and "variacao" in it and isinstance(it["variacao"], dict):
            out.append(it["variacao"])
        else:
            out.append(it)
    return out

def resolve_id(conta, sku, max_paginas=80):
    token = TOKENS.get(conta)
    sku = norm(sku)
    if not token or not sku:
        return None, "conta_ou_sku_invalido"

    # A) produto.pesquisar
    url_search = "https://api.tiny.com.br/api2/produto.pesquisar.php"
    print("\n[1/2] Tentando produto.pesquisar …", flush=True)
    st, js = post_json(url_search, {"pesquisa": sku}, token, conta, sku, "pesquisar")
    ret = (js or {}).get("retorno", {})
    if ret.get("status") == "OK":
        prods = ret.get("produtos") or []
        print(f"  resultados: {len(prods)}", flush=True)
        for it in prods:
            p = (it or {}).get("produto", {}) or {}
            pid = norm(p.get("id")); pcod = norm(p.get("codigo"))
            if pid and equal(pcod, sku):
                print("  ✓ bateu no produto", flush=True)
                return pid, "pesquisar_produto"
            for vv in iter_variacoes(p):
                vid = norm(vv.get("id")); vcod = norm(vv.get("codigo"))
                if vid and equal(vcod, sku):
                    print("  ✓ bateu na variação", flush=True)
                    return vid, "pesquisar_variacao"
    else:
        print("  (pesquisar não retornou OK, vamos para listar)", flush=True)

    # B) produto.listar (paginado)
    url_list = "https://api.tiny.com.br/api2/produto.listar.php"
    print("\n[2/2] Varredura em produto.listar (paginado) …", flush=True)
    for pagina in range(1, max_paginas+1):
        print(f"  página {pagina}…", flush=True)
        st, js = post_json(url_list, {"pagina": pagina}, token, conta, sku, f"listar_p{pagina:03d}")
        r = (js or {}).get("retorno", {})
        if r.get("status") != "OK":
            print(f"  status: {r.get('status')} (HTTP {st}) — continua tentando…", flush=True)
            if st in (404, 400):
                time.sleep(0.2)
                continue
            break
        itens = r.get("produtos") or []
        if not itens:
            print("  sem produtos; fim.", flush=True)
            break
        for it in itens:
            p = (it or {}).get("produto", {}) or {}
            pid = norm(p.get("id")); pcod = norm(p.get("codigo"))
            if pid and equal(pcod, sku):
                print("  ✓ achou produto com esse código", flush=True)
                return pid, f"listar_produto_p{pagina}"
            for vv in iter_variacoes(p):
                vid = norm(vv.get("id")); vcod = norm(vv.get("codigo"))
                if vid and equal(vcod, sku):
                    print("  ✓ achou variação com esse código", flush=True)
                    return vid, f"listar_variacao_p{pagina}"
        time.sleep(0.1)
    return None, "nao_encontrado"

def obter_estoque_geral(conta, id_):
    token = TOKENS.get(conta)
    url = "https://api.tiny.com.br/api2/produto.obter.estoque.php"
    print("\nObtendo produto.obter.estoque …", flush=True)
    st, js = post_json(url, {"id": id_}, token, conta, str(id_), "obter_estoque", timeout=12, tries=2)
    ret = (js or {}).get("retorno", {})
    if ret.get("status") != "OK":
        return None, ret.get("erros") or [{"erro":"status != OK"}]
    prod = ret.get("produto") or {}
    depos = prod.get("depositos") or {}
    if isinstance(depos, dict):
        lista = depos.get("deposito") or []
    elif isinstance(depos, list):
        lista = depos
    else:
        lista = []
    total_geral = 0.0
    dep_fmt = []
    for d in lista:
        nome = (d.get("nome") or "").strip()
        saldo = str(d.get("saldo") or "0").replace(",", ".")
        try: v = float(saldo)
        except: v = 0.0
        dep_fmt.append({"nome": nome, "saldo": v, "desconsiderar": str(d.get("desconsiderar") or "").upper()})
        if nome.upper() == "GERAL":
            total_geral += v
    return {"total_geral": int(total_geral), "depositos": dep_fmt}, None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("conta", choices=["ALIVVIA","JCA"])
    ap.add_argument("sku")
    ap.add_argument("--max-paginas", type=int, default=80)
    args = ap.parse_args()

    print(f"=== PROBE-QUICK | conta={args.conta} | sku={args.sku} ===", flush=True)

    _id, src = resolve_id(args.conta, args.sku, max_paginas=args.max_paginas)
    if not _id:
        print("\n❌ Não encontrei ID. Motivo:", src, flush=True)
        print("Confira os JSONs em ./logs/", flush=True)
        sys.exit(1)

    print(f"\n✓ ID resolvido: {_id} (via {src})", flush=True)

    data, err = obter_estoque_geral(args.conta, _id)
    if err:
        print("\n❌ Falha ao obter estoque:", err, flush=True)
        sys.exit(1)

    print("\n=== DEPÓSITOS ===", flush=True)
    for d in data["depositos"]:
        print(f"- nome={d['nome']} | saldo={int(d['saldo'])} | desconsiderar={d['desconsiderar']}", flush=True)
    print(f"\n>>> Soma do depósito 'Geral': {data['total_geral']}", flush=True)
    print("\n(Respostas salvas em ./logs)", flush=True)

if __name__ == "__main__":
    main()
