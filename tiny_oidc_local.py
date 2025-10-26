import http.server
import socketserver
import threading
import urllib.parse
import webbrowser
import time
import json
import requests

# ====== SUAS CHAVES (do Tiny) ======
CLIENT_ID = "tiny-api-0f4c1f7383e30bde76b72b3b94ad78a8ceb4cb70-1761414928"
CLIENT_SECRET = "KGwGqH2HwT55bDnUuI2azP4DfpPsPoXJ"
REDIRECT_URI = "http://localhost:8080/tiny/callback"
# ===================================

BASE_URL = "https://api.tiny.com.br"
AUTH_URL = f"{BASE_URL}/oauth2/authorize"
TOKEN_URL = f"{BASE_URL}/oauth2/token"

SCOPE = ""  # deixe vazio mesmo
TOKENS_FILE = "tokens_tiny.json"
oauth_code = {"code": None, "state": None}

class CallbackHandler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/tiny/callback":
            qs = urllib.parse.parse_qs(parsed.query)
            code = qs.get("code", [None])[0]
            state = qs.get("state", [None])[0]
            oauth_code["code"] = code
            oauth_code["state"] = state
            self.send_response(200)
            self.send_header("Content-type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(b"<h2>OK! Token de autorizacao recebido.</h2><p>Voce ja pode voltar ao PowerShell.</p>")
        else:
            self.send_response(404)
            self.end_headers()

def start_server():
    with socketserver.TCPServer(("localhost", 8080), CallbackHandler) as httpd:
        httpd.serve_forever()

def main():
    t = threading.Thread(target=start_server, daemon=True)
    t.start()

    state = str(int(time.time()))
    params = {
        "response_type": "code",
        "client_id": CLIENT_ID,
        "redirect_uri": REDIRECT_URI,
        "state": state,
        "prompt": "login",  # força tela de login (evita sessão antiga)
    }
    url = AUTH_URL + "?" + urllib.parse.urlencode(params)

    print("\nAbra essa URL em janela ANÔNIMA (copie/cole):\n")
    print(url, "\n")

    try:
        webbrowser.open(url)
    except:
        pass

    print("Aguardando o Tiny redirecionar para o callback...")
    while oauth_code["code"] is None:
        time.sleep(0.5)

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
    print("Status:", resp.status_code)
    try:
        js = resp.json()
        print(json.dumps(js, indent=2, ensure_ascii=False))
    except:
        print(resp.text)
        return

    if "access_token" in js:
        with open(TOKENS_FILE, "w", encoding="utf-8") as f:
            json.dump(js, f, indent=2, ensure_ascii=False)
        print(f"\n✅ Tokens salvos em: {TOKENS_FILE}")
    else:
        print("\n❌ Nao recebi access_token. Revise Client ID/Secret/Redirect.")

if __name__ == "__main__":
    main()
