# tiny_oauth_local.py
# Fluxo OAuth do Tiny (novo) usando "accounts.tiny.com.br" + servidor local p/ receber o callback

import http.server
import socketserver
import threading
import urllib.parse
import webbrowser
import time
import json
import base64
import requests

# ======= PREENCHA AQUI (copie do portal do Tiny) =========================
CLIENT_ID = "tiny-api-0f4c1f7383e30bde76b72b3b94ad78a8ceb4cb70-1761414928"
CLIENT_SECRET = "GKIcZ2dfRbMpwPDjQeAmywrlsdGwdFwJ"
# Tem que ser exatamente o mesmo que está no portal:
REDIRECT_URI = "http://localhost:8080/tiny/callback"
# ========================================================================

# Endpoints do novo OAuth do Tiny (Keycloak)
AUTH_URL  = "https://accounts.tiny.com.br/realms/tiny/protocol/openid-connect/auth"
TOKEN_URL = "https://accounts.tiny.com.br/realms/tiny/protocol/openid-connect/token"

# Porta do servidor local (mantenha 8080 para bater com o REDIRECT_URI)
PORT = 8080

TOKENS_FILE = "tokens_tiny.json"
oauth_code  = {"code": None, "state": None}

class CallbackHandler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/tiny/callback":
            qs = urllib.parse.parse_qs(parsed.query)
            oauth_code["code"]  = qs.get("code",  [None])[0]
            oauth_code["state"] = qs.get("state", [None])[0]
            self.send_response(200)
            self.send_header("Content-type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(
                b"<h2>OK! Token de autorizacao recebido.</h2><p>Volte ao PowerShell.</p>"
            )
        else:
            self.send_response(404)
            self.end_headers()

def start_server():
    with socketserver.TCPServer(("localhost", PORT), CallbackHandler) as httpd:
        httpd.serve_forever()

def main():
    # 1) Sobe o servidor local do callback
    t = threading.Thread(target=start_server, daemon=True)
    t.start()

    # 2) Monta a URL de autorizacao no "accounts"
    state = str(int(time.time()))
    params = {
        "response_type": "code",
        "client_id": CLIENT_ID,
        "redirect_uri": REDIRECT_URI,
        "state": state,
        # escopos básicos; se não precisar, pode deixar só "openid"
        SCOPE = ""          # solução mais simples
# ou, se quiser testar: SCOPE = "openid",
        # força a tela de login para evitar "sessao expirada"
        "prompt": "login"
    }
    url = AUTH_URL + "?" + urllib.parse.urlencode(params)

    print("\nAbra esta URL (copie/cole) em janela ANÔNIMA:")
    print(url, "\n")

    # tenta abrir automático
    try:
        webbrowser.open(url)
    except:
        pass

    # 3) Espera o Tiny redirecionar para o callback
    print("Aguardando o redirecionamento para", REDIRECT_URI, "...")
    while oauth_code["code"] is None:
        time.sleep(0.3)

    # 4) Troca "code" por access_token/refresh_token (no /token do accounts)
    basic = base64.b64encode(f"{CLIENT_ID}:{CLIENT_SECRET}".encode()).decode()
    headers = {
        "Authorization": f"Basic {basic}",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    data = {
        "grant_type": "authorization_code",
        "code": oauth_code["code"],
        "redirect_uri": REDIRECT_URI,
    }
    resp = requests.post(TOKEN_URL, data=data, headers=headers)

    print("\n=== RESPOSTA DO TOKEN ===")
    print("Status:", resp.status_code)
    try:
        js = resp.json()
        print(json.dumps(js, indent=2, ensure_ascii=False))
    except Exception:
        print(resp.text)
        return

    if "access_token" in js:
        with open(TOKENS_FILE, "w", encoding="utf-8") as f:
            json.dump(js, f, indent=2, ensure_ascii=False)
        print(f"\n✅ Tokens salvos em: {TOKENS_FILE}")
    else:
        print("\n❌ Não veio access_token. Revise CLIENT_ID/SECRET/REDIRECT_URI e permissões do app.")

if __name__ == "__main__":
    main()
