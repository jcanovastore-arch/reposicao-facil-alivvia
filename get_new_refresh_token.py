# get_new_refresh_token.py
# Uso:
#   python get_new_refresh_token.py --empresa ALIVVIA --client_id <...> --client_secret <...> --redirect_uri http://localhost/callback
#   (abre a URL no navegador, você faz login no Tiny e copia/cola a URL de redirecionamento completa aqui no prompt)
#
# O script:
# 1) Gera PKCE (code_verifier/code_challenge)
# 2) Monta a URL de autorização do Tiny (com escopo offline_access para emitir refresh_token)
# 3) Você cola a URL de retorno (com ?code=...), ele troca por tokens e salva o novo refresh_token em tiny_secrets.json

import base64, hashlib, os, json, argparse, sys, urllib.parse, requests

AUTH_BASE = "https://accounts.tiny.com.br/realms/tiny/protocol/openid-connect/auth"
TOKEN_URL = "https://accounts.tiny.com.br/realms/tiny/protocol/openid-connect/token"
JSON_PATH = "tiny_secrets.json"

def b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode().rstrip("=")

def gen_pkce():
    verifier = b64url(os.urandom(32))
    challenge = b64url(hashlib.sha256(verifier.encode()).digest())
    return verifier, challenge

def load_json():
    if not os.path.exists(JSON_PATH):
        return {}
    with open(JSON_PATH, "r", encoding="utf-8") as f:
        try:
            return json.load(f)
        except Exception:
            return {}

def save_json(data):
    with open(JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--empresa", required=True, choices=["ALIVVIA","JCA"])
    ap.add_argument("--client_id", required=True)
    ap.add_argument("--client_secret", required=True)
    ap.add_argument("--redirect_uri", required=True, help="Deve estar cadastrado no Tiny (ex.: http://localhost/callback)")
    args = ap.parse_args()

    code_verifier, code_challenge = gen_pkce()

    # URL de autorização (pedimos offline_access para receber refresh_token)
    params = {
        "client_id": args.client_id,
        "response_type": "code",
        "scope": "openid offline_access email",
        "redirect_uri": args.redirect_uri,
        "code_challenge": code_challenge,
        "code_challenge_method": "S256"
    }
    auth_url = AUTH_BASE + "?" + urllib.parse.urlencode(params)
    print("\nAbra esta URL no navegador, faça login e autorize:")
    print(auth_url)
    print("\nDepois de logar, você será redirecionado para a redirect_uri.")
    print("Copie a URL COMPLETA que apareceu na barra do navegador e cole aqui.")
    redir = input("\nCole a URL de redirecionamento completa: ").strip()

    # Extrai o ?code=...
    parsed = urllib.parse.urlparse(redir)
    q = urllib.parse.parse_qs(parsed.query)
    code = (q.get("code") or [None])[0]
    if not code:
        print("Não encontrei 'code' na URL colada. Verifique e tente novamente.")
        sys.exit(1)

    # Troca o code por tokens
    data = {
        "grant_type": "authorization_code",
        "client_id": args.client_id,
        "client_secret": args.client_secret,
        "code": code,
        "redirect_uri": args.redirect_uri,
        "code_verifier": code_verifier
    }
    r = requests.post(TOKEN_URL, data=data, timeout=60)
    if not r.ok:
        print("Falha ao trocar code por token:", r.status_code, r.text[:400])
        sys.exit(1)
    tok = r.json()
    refresh_token = tok.get("refresh_token")
    if not refresh_token:
        print("Resposta sem refresh_token. Verifique escopos/redirect na app do Tiny.")
        sys.exit(1)

    store = load_json()
    if args.empresa not in store:
        store[args.empresa] = {}
    store[args.empresa]["client_id"] = args.client_id
    store[args.empresa]["client_secret"] = args.client_secret
    store[args.empresa]["refresh_token"] = refresh_token
    save_json(store)

    print(f"\n✅ Novo refresh_token salvo em {JSON_PATH} para {args.empresa}.")
    print("Agora rode o teste de variações novamente.")

if __name__ == "__main__":
    main()
