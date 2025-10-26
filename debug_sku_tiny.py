# debug_sku_tiny.py — v1.2
# Resolve ID de produto no Tiny por 3 rotas (pesquisar → obter(codigo) → listar)
# e mede estoque por ID (robusto) e por CODIGO (comparativo), salvando JSONs.

import os, sys, time, json, csv, argparse
from datetime import datetime
import requests
from unidecode import unidecode

TOKENS = {
    "ALIVVIA": "b3ca9c3319ac75276c03e097296e15619259cab9029a1e45b781a07553bdb25b",
    "JCA":     "352880e9498ec1a29b81a9f0ea1a946a46415f93b2aa2706634f39064b750dcd",
}

def _norm(s): return unidecode(str(s or "")).strip().lower()

def br_to_float(x):
    if x is None: return 0.0
    if isinstance(x, (int, float)): return float(x)
    s = str(x).strip().replace("\u00a0"," ").replace("R$","").replace(" ","")
    if "," in s and "." not in s: s = s.replace(",", ".")
    elif "," in s and "." in s:  s = s.replace(".", "").replace(",", ".")
    try: return float(s)
    except: return 0.0

class Tiny:
    def __init__(self, token, timeout=30):
        self.token = token
        self.s = requests.Session()
        self.s.headers.update({"User-Agent":"Tiny-Probe/1.2"})
        self.timeout = timeout

    def _post(self, url, data):
        data = {**data, "token": self.token, "formato": "json"}
        r = self.s.post(url, data=data, timeout=self.timeout)
        try: js = r.json()
        except Exception: js = {"raw": r.text[:1500]}
        return r.status_code, js

    # ------- produtos -------
    def produto_pesquisar(self, termo):
        return self._post("https://api.tiny.com.br/api2/produto.pesquisar.php", {"pesquisa": termo})

    def produto_obter_por_codigo(self, codigo):
        # em algumas contas NÃO funciona; retorna erro pedindo 'id'
        return self._post("https://api.tiny.com.br/api2/produto.obter.php", {"codigo": codigo})

    def produto_listar(self, pagina=1, pesquisa=None):
        data = {"pagina": pagina}
        if pesquisa:
            data["pesquisa"] = pesquisa
        return self._post("https://api.tiny.com.br/api2/produto.listar.php", data)

    # ------- estoque -------
    def estoque_por_id(self, pid):
        return self._post("https://api.tiny.com.br/api2/produto.obter.estoque.php", {"id": pid})

    def estoque_por_codigo(self, codigo):
        # algumas contas aceitam; fica como comparativo
        return self._post("https://api.tiny.com.br/api2/produto.obter.estoque.php", {"codigo": codigo})

def salvar_json(base_dir, conta, sku, tag, payload):
    os.makedirs(base_dir, exist_ok=True)
    fn = os.path.join(base_dir, f"{conta}_{sku}_{tag}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
    try:
        with open(fn, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        return fn
    except Exception:
        return None

def soma_geral(depositos):
    total = 0.0
    if isinstance(depositos, list):
        for d in depositos:
            if _norm(d.get("nome")) != "geral":  # só depósito Geral
                continue
            if str(d.get("desconsiderar","N")).strip().upper() == "S":
                continue
            v = d.get("saldo", None)
            if v in (None, "", "None"): v = d.get("saldo_disponivel", None)
            if v in (None, "", "None"): v = d.get("qtd", 0)
            total += br_to_float(v)
    return int(round(max(0, total)))

def lista_depositos(depositos):
    if not isinstance(depositos, list): return "(sem depósitos)"
    out = []
    for d in depositos:
        nome = d.get("nome"); desc = d.get("desconsiderar","N")
        v = d.get("saldo", None)
        if v in (None, "", "None"): v = d.get("saldo_disponivel", None)
        if v in (None, "", "None"): v = d.get("qtd", 0)
        out.append(f" - {nome!r:>25} | desconsiderar={desc} | saldo={v}")
    return "\n".join(out) if out else "(sem depósitos)"

def resolver_id(cli: Tiny, sku: str, verbose=True, max_paginas=80):
    """Tenta achar o ID do produto com codigo == sku (exato)."""
    # 1) pesquisar
    st, js = cli.produto_pesquisar(sku)
    salvar_json("logs", "RESOLVE", sku, "pesquisar", js)
    ret = js.get("retorno", {}) if isinstance(js, dict) else {}
    itens = ret.get("produtos", []) or []
    for it in itens:
        p = it.get("produto", {})
        if str(p.get("codigo","")).strip().upper() == sku.upper():
            if verbose: print("ID encontrado via pesquisar.")
            return p.get("id"), p.get("nome")

    # 2) obter por codigo (pode falhar nessa conta)
    st2, js2 = cli.produto_obter_por_codigo(sku)
    salvar_json("logs", "RESOLVE", sku, "obter_por_codigo", js2)
    ret2 = js2.get("retorno", {}) if isinstance(js2, dict) else {}
    prod = ret2.get("produto", {}) or {}
    if prod.get("id"):
        if verbose: print("ID encontrado via obter(codigo).")
        return prod.get("id"), prod.get("nome")

    # 3) listar paginado com filtro de pesquisa
    if verbose: print("Buscando via produto.listar (paginado)…")
    pagina = 1
    while pagina <= max_paginas:
        st3, js3 = cli.produto_listar(pagina=pagina, pesquisa=sku)
        salvar_json("logs", "RESOLVE", sku, f"listar_p{pagina}", js3)
        ret3 = js3.get("retorno", {}) if isinstance(js3, dict) else {}
        prods = ret3.get("produtos", []) or []
        for it in prods:
            p = it.get("produto", {})
            if str(p.get("codigo","")).strip().upper() == sku.upper():
                if verbose: print(f"ID encontrado via listar (página {pagina}).")
                return p.get("id"), p.get("nome")
        # parar se acabou
        if not prods or len(prods) == 0:
            break
        pagina += 1

    return None, None

def stats(arr):
    a = sorted(arr)
    if not a: return (0,0,0)
    n = len(a)
    med = a[n//2] if n%2==1 else int(round((a[n//2-1]+a[n//2])/2))
    return (min(a), med, max(a))

def probe_um_sku(conta, sku, leituras=5, delay=1.0):
    token = TOKENS.get(conta.upper())
    if not token:
        print(f"[ERRO] Conta '{conta}' sem token.")
        return 1
    cli = Tiny(token)

    print(f"=== PROBE | conta={conta} | sku={sku} | leituras={leituras} ===")
    pid, nome = resolver_id(cli, sku)
    print(f"Resolver ID → id={pid} | nome={nome}")

    geral_id, geral_cod, precos = [], [], []

    for i in range(1, leituras+1):
        # por ID (se encontrado)
        if pid:
            st_id, js_id = cli.estoque_por_id(pid)
            ret_id = js_id.get("retorno", {}) if isinstance(js_id, dict) else {}
            deps_id = ret_id.get("depositos", []) or []
            g_id = soma_geral(deps_id)
            precos.append(br_to_float(ret_id.get("produto", {}).get("preco_custo_medio", 0)))
            fn_id = salvar_json("logs", conta, sku, f"read{i}_byID", js_id)
            print(f"{i:02d}) byID:  HTTP {st_id} | Geral={g_id} | json={os.path.basename(fn_id) if fn_id else '-'}")
            if deps_id: print(lista_depositos(deps_id))
            geral_id.append(g_id)
        else:
            print(f"{i:02d}) byID:  (sem id resolvido)")

        # comparativo por CODIGO
        st_cd, js_cd = cli.estoque_por_codigo(sku)
        ret_cd = js_cd.get("retorno", {}) if isinstance(js_cd, dict) else {}
        if ret_cd.get("status") and ret_cd.get("status").lower() != "ok":
            print(f"    byCOD: status={ret_cd.get('status')} | erros={ret_cd.get('erros')}")
        deps_cd = ret_cd.get("depositos", []) or []
        g_cd = soma_geral(deps_cd)
        fn_cd = salvar_json("logs", conta, sku, f"read{i}_byCOD", js_cd)
        print(f"    byCOD: HTTP {st_cd} | Geral={g_cd} | json={os.path.basename(fn_cd) if fn_cd else '-'}")
        if deps_cd: print(lista_depositos(deps_cd))
        geral_cod.append(g_cd)

        time.sleep(delay)

    id_min, id_med, id_max = stats(geral_id)
    cd_min, cd_med, cd_max = stats(geral_cod)
    preco_med = sum(precos)/len(precos) if precos else 0.0

    print("\n=== RESUMO ===")
    if geral_id:
        print(f"Geral (by ID):  min={id_min} | mediana={id_med} | max={id_max}")
    else:
        print("Geral (by ID):  (sem leituras — id não encontrado)")
    print(f"Geral (by COD): min={cd_min} | mediana={cd_med} | max={cd_max}")
    print(f"Preço médio (preco_custo_medio): {preco_med:.2f}")
    return 0

def probe_batch(conta, arquivo_csv, saida_csv="resultado_batch.csv", leituras=3, delay=0.8):
    skus = []
    with open(arquivo_csv, newline="", encoding="utf-8") as f:
        rd = csv.DictReader(f)
        if "SKU" in (rd.fieldnames or []):
            for row in rd:
                s = str(row["SKU"]).strip()
                if s: skus.append(s)
        else:
            f.seek(0); rd2 = csv.reader(f)
            for row in rd2:
                if row:
                    s = str(row[0]).strip()
                    if s and s.upper()!="SKU": skus.append(s)

    token = TOKENS.get(conta.upper())
    if not token:
        print(f"[ERRO] Conta '{conta}' sem token.")
        return 1
    cli = Tiny(token)

    out = []
    for sku in skus:
        pid, _ = resolver_id(cli, sku, verbose=False)
        g_id, g_cd = [], []
        for _i in range(leituras):
            if pid:
                st1, js1 = cli.estoque_por_id(pid)
                deps1 = js1.get("retorno",{}).get("depositos",[]) if isinstance(js1,dict) else []
                g_id.append(soma_geral(deps1))
            st2, js2 = cli.estoque_por_codigo(sku)
            deps2 = js2.get("retorno",{}).get("depositos",[]) if isinstance(js2,dict) else []
            g_cd.append(soma_geral(deps2))
            time.sleep(delay)

        def med(arr):
            a = sorted(arr)
            if not a: return 0
            n = len(a)
            return a[n//2] if n%2==1 else int(round((a[n//2-1]+a[n//2])/2))

        out.append({
            "SKU": sku,
            "byID_min": min(g_id) if g_id else 0,
            "byID_med": med(g_id),
            "byID_max": max(g_id) if g_id else 0,
            "byCOD_min": min(g_cd) if g_cd else 0,
            "byCOD_med": med(g_cd),
            "byCOD_max": max(g_cd) if g_cd else 0,
        })
        print(f"[{conta}] {sku}: byID_med={out[-1]['byID_med']} | byCOD_med={out[-1]['byCOD_med']}")

    with open(saida_csv, "w", newline="", encoding="utf-8") as f:
        wr = csv.DictWriter(f, fieldnames=list(out[0].keys()))
        wr.writeheader(); wr.writerows(out)
    print(f"✔ Arquivo salvo: {saida_csv}")
    return 0

def main():
    ap = argparse.ArgumentParser(description="Probe de SKUs no Tiny (resolve ID por pesquisar/obter/listar)")
    ap.add_argument("conta", help="ALIVVIA ou JCA")
    ap.add_argument("--sku", help="SKU único para diagnosticar")
    ap.add_argument("--csv", help="Arquivo CSV com coluna SKU")
    ap.add_argument("--leituras", type=int, default=5)
    ap.add_argument("--delay", type=float, default=1.0)
    ap.add_argument("--saida", default="resultado_batch.csv")
    args = ap.parse_args()

    if args.sku and args.csv:
        print("Use --sku OU --csv (apenas um)."); return 2
    if args.sku:
        return probe_um_sku(args.conta, args.sku, leituras=args.leituras, delay=args.delay)
    elif args.csv:
        return probe_batch(args.conta, args.csv, saida_csv=args.saida, leituras=args.leituras, delay=args.delay)
    else:
        print("Informe --sku <SKU> ou --csv <arquivo.csv>."); return 2

if __name__ == "__main__":
    sys.exit(main())
