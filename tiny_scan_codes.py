# tiny_scan_codes.py
import os, sys, time, csv, json, argparse, requests

TOKENS = {
    "ALIVVIA": "b3ca9c3319ac75276c03e097296e15619259cab9029a1e45b781a07553bdb25b",
    "JCA":     "352880e9498ec1a29b81a9f0ea1a946a46415f93b2aa2706634f39064b750dcd",
}

S = requests.Session()
S.headers.update({"User-Agent": "Alivvia-Tiny-Scan/1.0"})

def post_json(url, data, token, timeout=12):
    data = {**data, "token": token, "formato": "json"}
    r = S.post(url, data=data, timeout=timeout)
    try:
        return r.status_code, r.json()
    except Exception:
        return r.status_code, {"_text": r.text[:1000]}

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

def scan(conta, max_paginas, out_csv):
    token = TOKENS.get(conta)
    if not token:
        print("Conta inválida.")
        sys.exit(1)

    url_list = "https://api.tiny.com.br/api2/produto.listar.php"
    rows = []
    total_ok_pages = 0

    for pagina in range(1, max_paginas+1):
        print(f"página {pagina}…", flush=True)
        st, js = post_json(url_list, {"pagina": pagina}, token, timeout=12)
        ret = (js or {}).get("retorno", {})
        if ret.get("status") != "OK":
            # 404 aqui geralmente significa "página vazia"
            if st in (404, 400) or ret.get("status") is None:
                break
            print(f"  status={ret.get('status')} HTTP={st} -> parando.")
            break

        total_ok_pages += 1
        itens = ret.get("produtos") or []
        if not itens:
            break

        for it in itens:
            p = (it or {}).get("produto", {}) or {}
            pid = str(p.get("id") or "").strip()
            pcod = str(p.get("codigo") or "").strip()
            if pid or pcod:
                rows.append([pcod, pid, "produto"])
            for vv in iter_variacoes(p):
                vid = str(vv.get("id") or "").strip()
                vcod = str(vv.get("codigo") or "").strip()
                if vid or vcod:
                    rows.append([vcod, vid, "variacao"])

        time.sleep(0.05)  # educado com a API

    # grava CSV
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["codigo", "id", "tipo"])
        w.writerows(rows)

    print(f"\n✓ Concluído. Páginas OK: {total_ok_pages}. Linhas: {len(rows)}.")
    print(f"Arquivo: {out_csv}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("conta", choices=["ALIVVIA", "JCA"])
    ap.add_argument("--max", type=int, default=200)
    ap.add_argument("--out", default="tiny_codes.csv")
    args = ap.parse_args()
    scan(args.conta, args.max, args.out)
