# test_obter_by_codigo.py
# Testa produto.obter.php passando codigo=<SKU> e imprime a resposta integral.

import sys, json, requests

TOKENS = {
    "ALIVVIA": "b3ca9c3319ac75276c03e097296e15619259cab9029a1e45b781a07553bdb25b",
    "JCA":     "352880e9498ec1a29b81a9f0ea1a946a46415f93b2aa2706634f39064b750dcd",
}

def main():
    if len(sys.argv) < 3:
        print("Uso: python test_obter_by_codigo.py <ALIVVIA|JCA> <SKU>")
        sys.exit(0)

    conta = sys.argv[1].upper()
    sku   = " ".join(sys.argv[2:]).strip()
    token = TOKENS.get(conta)
    if not token:
        print("Conta inválida.")
        sys.exit(1)

    url = "https://api.tiny.com.br/api2/produto.obter.php"
    data = {"token": token, "formato": "json", "codigo": sku}

    print(f"→ POST {url} | conta={conta} | codigo={sku}")
    s = requests.Session()
    s.headers.update({"User-Agent": "Tiny-Test/obter-by-codigo"})
    r = s.post(url, data=data, timeout=60)

    print(f"HTTP {r.status_code}")
    ct = r.headers.get("Content-Type", "")
    print(f"Content-Type: {ct}")

    text = r.text
    try:
        js = r.json()
        print("JSON (formatado):")
        print(json.dumps(js, indent=2, ensure_ascii=False))
    except Exception:
        print("Resposta não-JSON (primeiros 1000 chars):")
        print(text[:1000])

if __name__ == "__main__":
    main()
