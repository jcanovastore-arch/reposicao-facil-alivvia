import os, json, requests, time

# === CONFIGURAÇÃO INICIAL ===
CLIENTS = {
    "alivvia": {
        "client_id": "tiny-api-f6f6bb60d310a378fc0d68952963ef2408ad15b7-1761425352",
        "client_secret": "i2R2Nw7VvfdWtUx13AEUCb0FQCcmWDGp",
    },
    "jca": {
        "client_id": "tiny-api-0f4c1f7383e30bde76b72b3b94ad78a8ceb4cb70-1761414928",
        "client_secret": "yaIipiTUDdeSlsy43DKwox2koH6YvQVq",
    },
}

TOKEN_DIR = os.path.join(os.path.dirname(__file__), "tokens")
os.makedirs(TOKEN_DIR, exist_ok=True)

def refresh_token(emp):
    """Atualiza o token de uma empresa"""
    path = os.path.join(TOKEN_DIR, f"tiny_{emp}.json")
    if not os.path.exists(path):
        print(f"❌ Token inicial não encontrado: {path}")
        return

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    refresh_token = data.get("refresh_token")
    if not refresh_token:
        print(f"⚠️ Sem refresh_token válido para {emp}. Faça login manual uma vez.")
        return

    creds = CLIENTS[emp]
    url = "https://accounts.tiny.com.br/realms/tiny/protocol/openid-connect/token"
    payload = {
        "grant_type": "refresh_token",
        "client_id": creds["client_id"],
        "client_secret": creds["client_secret"],
        "refresh_token": refresh_token,
    }

    r = requests.post(url, data=payload)
    if not r.ok:
        print(f"⚠️ Erro ao atualizar {emp}: {r.status_code} {r.text[:150]}")
        return

    new_tokens = r.json()
    with open(path, "w", encoding="utf-8") as f:
        json.dump(new_tokens, f, ensure_ascii=False, indent=2)

    print(f"✅ Token atualizado com sucesso para {emp} ({time.strftime('%H:%M:%S')})")

if __name__ == "__main__":
    for empresa in CLIENTS.keys():
        refresh_token(empresa)
