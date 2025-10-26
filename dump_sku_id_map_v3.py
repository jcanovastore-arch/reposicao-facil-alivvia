# -*- coding: utf-8 -*-
import os, csv, time, json, random, sys
import requests

BASE = "https://erp.tiny.com.br/public-api/v3"
SESS = requests.Session()

def ensure_dirs():
    os.makedirs("tokens", exist_ok=True)
    os.makedirs("dados", exist_ok=True)

def load_token(conta):
    path = os.path.join("tokens", f"tokens_tiny_{conta}.json")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)["access_token"]

def headers(conta):
    return {"Authorization": f"Bearer {load_token(conta)}"}

def get_json(path, conta, params=None):
    """GET com retry simples para 429."""
    url = f"{BASE}{path}"
    tentativa = 0
    while True:
        r = SESS.get(url, headers=headers(conta), params=params, timeout=60)
        if r.status_code == 429:
            espera = min(2 ** min(tentativa, 5) + random.uniform(0, 0.4), 15)
            tentativa += 1
            print(f"   429 (rate limit). Aguardando {espera:.1f}s…")
            time.sleep(espera)
            continue
        r.raise_for_status()
        if not r.content:
            return {}
        return r.json()

def dump_sku_id(conta, delay=0.4):
    """Percorre /produtos e salva CSV com SKU, ID, Nome."""
    print(f"\n=== {conta} ===")
    saida = os.path.join("dados", f"sku_id_{conta}.csv")
    total = 0
    page, limit = 1, 100

    with open(saida, "w", newline="", encoding="utf-8") as f:
        wr = csv.writer(f)
        wr.writerow(["SKU", "ID", "Nome"])
        while True:
            js = get_json("/produtos", conta, params={"page": page, "limit": limit})
            itens = js.get("itens") or []
            if not itens:
                break
            for p in itens:
                sku = (p.get("codigo") or "").strip()
                pid = p.get("id")
                nome = (p.get("nome") or "").strip()
                if sku and pid:
                    wr.writerow([sku, pid, nome])
                    total += 1
            print(f"  página {page}… acumulado {total}")
            page += 1
            if len(itens) < limit:
                break
            time.sleep(delay)  # pausa curta para evitar 429

    print(f"✔ Gerado: {saida} ({total} linhas)")

def main():
    ensure_dirs()
    # Uso: python dump_ids_simple.py ALIVVIA   ou   python dump_ids_simple.py JCA
    if len(sys.argv) != 2 or sys.argv[1] not in ("ALIVVIA", "JCA"):
        print("Uso: python dump_ids_simple.py ALIVVIA | JCA")
        sys.exit(1)
    conta = sys.argv[1]
    dump_sku_id(conta)

if __name__ == "__main__":
    main()
