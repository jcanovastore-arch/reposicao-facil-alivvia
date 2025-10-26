# tiny_connect_v3.py
import http.server, socketserver, threading, urllib.parse, webbrowser
import time, json, base64, requests

# >>> PREENCHA AQUI <<<
CLIENT_ID     = "tiny-api-f6f6bb60d310a378fc0d68952963ef2408ad15b7-1761425352"
CLIENT_SECRET = "i2R2Nw7VvfdWtUx13AEUCb0FQCcmWDGp"
REDIRECT_URI  = "http://localhost:8080/tiny/callback"
# ESCOPOS – tem que ter openid
SCOPE = "openid email offline_access"

# Endpoints do OIDC v3 do Tiny
AUTH_URL  = "https://accounts.tiny.com.br/realms/tiny/protocol/openid-connect/auth"
TOKEN_URL = "https://accounts.tiny.com.br/realms/tiny/protocol/openid-connect/token"

TOKENS_FILE = "tokens/tokens_tiny_ALIVVIA.json"
oauth_code  = {"code": None, "error": None, "error_description": None}

class CallbackHandler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path != "/tiny/callback":
            self.send_response(404); self.end_headers(); return

        qs = urllib.parse.parse_qs(parsed.query)
        if "error" in qs:
            oauth_code["error"] = qs.get("error",[""])[0]
            oauth_code["error_description"] = qs.get("error_description",[""])[0]
            msg = f"<h1>Erro no callback</h1><p>{oauth_code['error']}</p><p>{oauth_code['error_description']}</p>"
            self.send_response(200); self.send_header("Content-Type","text/html; charset=utf-8"); self.end_headers()
            self.wfile.write(msg.encode("utf-8")); return

        code = qs.get("code",[None])[0]
        oauth_code["code"] = code
        self.send_response(200)
        self.send_header("Content-Type","text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write(b"<h1>OK! Token de autorizacao recebido.</h1><p>Volte ao PowerShell.</p>")

def start_server():
    with socketserver.TCPServer(("localhost", 8080), CallbackHandler) as httpd:
        httpd.serve_forever()

def main():
    # 1) Sobe o servidor local (callback)
    t = threading.Thread(target=start_server, daemon=True)
    t.start()

    # 2) Monta URL de autorização (sem scope)
    state = str(int(time.time()))
    params = {
        "response_type": "code",
        "client_id": CLIENT_ID,
        "redirect_uri": REDIRECT_URI,
        "state": state,
        "prompt": "login",
    }
    if SCOPE:
        params["scope"] = SCOPE  # (não será usado)

    url = AUTH_URL + "?" + urllib.parse.urlencode(params)

    print("\nCOPIE E COLE ESTA URL em janela ANÔNIMA:\n")
    print(url, "\n")
    print("Aguardando o Tiny redirecionar para o callback...\n")

    try: webbrowser.open(url)
    except: pass

    while oauth_code["code"] is None and oauth_code["error"] is None:
        time.sleep(0.2)

    if oauth_code["error"]:
        print("❌ Erro no callback:")
        print(oauth_code["error"], oauth_code["error_description"])
        return

    # 3) Troca CODE por TOKENS (OIDC v3)
    auth_header = base64.b64encode(f"{CLIENT_ID}:{CLIENT_SECRET}".encode()).decode()
    headers = {
        "Authorization": f"Basic {auth_header}",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    data = {
        "grant_type": "authorization_code",
        "code": oauth_code["code"],
        "redirect_uri": REDIRECT_URI,
    }

    resp = requests.post(TOKEN_URL, headers=headers, data=data)
    print("\n=== RESPOSTA DO TOKEN ===")
    try:
        js = resp.json()
        print(json.dumps(js, indent=2, ensure_ascii=False))
    except:
        print(resp.text)
        return

    if "access_token" in js:
        with open(TOKENS_FILE, "w", encoding="utf-8") as f:
            json.dump(js, f, indent=2, ensure_ascii=False)
        print(f"\n✅ Tokens salvos em {TOKENS_FILE}")
    else:
        print("\n❌ Não veio access_token. Verifique Client ID/Secret/Redirect URI.")

if __name__ == "__main__":
    main()
