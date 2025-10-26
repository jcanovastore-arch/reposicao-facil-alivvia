# tiny_connect.py  (COLE TUDO e salve)
import http.server, socketserver, threading, urllib.parse, webbrowser, time, json, requests

# ====== PREENCHA AQUI ======
CLIENT_ID = "tiny-api-0f4c1f7383e30bde76b72b3b94ad78a8ceb4cb70-1761414928"
CLIENT_SECRET = "GKIcZ2dfRbMpwPDjQeAmywrlsdGwdFwJ"
REDIRECT_URI = "http://localhost:8080/tiny/callback"
# ===========================

BASE_URL = "https://api.tiny.com.br"
AUTH_URL = f"{BASE_URL}/oauth2/authorize"
TOKEN_URL = f"{BASE_URL}/oauth2/token"
SCOPE = ""  # deixe VAZIO (assim evita o erro invalid_scope)

TOKENS_FILE = "tokens_tiny.json"
oauth_code = {"code": None}

class CallbackHandler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/tiny/callback":
            qs = urllib.parse.parse_qs(parsed.query)

            # se voltar com erro, mostro a mensagem
            if "error" in qs:
                self.send_response(200)
                self.send_header("Content-type", "text/html; charset=utf-8")
                self.end_headers()
                err = qs.get("error",[""])[0]
                desc = qs.get("error_description",[""])[0]
                self.wfile.write(f"<h3>Erro: {err}</h3><p>{desc}</p>".encode())
                return

            oauth_code["code"] = qs.get("code", [None])[0]
            self.send_response(200)
            self.send_header("Content-type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(b"<h2>OK! Token de autorizacao recebido.</h2><p>Volte ao PowerShell.</p>")
        else:
            self.send_response(404); self.end_headers()

def start_server():
    with socketserver.TCPServer(("localhost", 8080), CallbackHandler) as httpd:
        httpd.serve_forever()

def main():
    # 1) liga o servidor local
    t = threading.Thread(target=start_server, daemon=True); t.start()

    # 2) monta a URL de login (sempre forca tela de login)
    params = {
        "response_type": "code",
        "client_id": CLIENT_ID,
        "redirect_uri": REDIRECT_URI,
        "prompt": "login",
    }
    if SCOPE: params["scope"] = SCOPE
    url = AUTH_URL + "?" + urllib.parse.urlencode(params)

    print("\nCOPIE E COLE ESTA URL em janela ANÔNIMA:\n")
    print(url, "\n")

    # tenta abrir sozinho
    try: webbrowser.open(url)
    except: pass

    print("Aguardando o Tiny redirecionar para o callback...")
    while oauth_code["code"] is None:
        time.sleep(0.3)

    # 3) troca code por tokens
    data = {
        "grant_type": "authorization_code",
        "code": oauth_code["code"],
        "redirect_uri": REDIRECT_URI,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    resp = requests.post(TOKEN_URL, data=data, headers=headers)

    print("\n=== RESPOSTA DO TOKEN ===")
    try:
        js = resp.json()
    except:
        print(resp.text)
        return

    print(json.dumps(js, indent=2, ensure_ascii=False))

    if "access_token" in js:
        with open(TOKENS_FILE, "w", encoding="utf-8") as f:
            json.dump(js, f, indent=2, ensure_ascii=False)
        print(f"\n✅ Tokens salvos em: {TOKENS_FILE}")
    else:
        print("\n❌ Não veio access_token. Verifique ID/Secret/Redirect URI.")

if __name__ == "__main__":
    main()
