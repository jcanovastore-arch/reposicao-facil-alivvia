# testar_tokens.py
import json, time, base64
import requests
from pathlib import Path

# Arquivos de token (ajuste o caminho se estiver diferente)
TOKENS = [
    ("JCA",      Path("tokens") / "tokens_tiny_JCA.json"),
    ("ALIVVIA",  Path("tokens") / "tokens_tiny_ALIVVIA.json"),
]

USERINFO_URL = "https://accounts.tiny.com.br/realms/tiny/protocol/openid-connect/userinfo"

def decode_jwt_payload(jwt: str) -> dict:
    # payload é a 2ª parte do JWT
    payload_b64 = jwt.split(".")[1]
    # padding para base64url
    padding = "=" * (-len(payload_b64) % 4)
    data = base64.urlsafe_b64decode(payload_b64 + padding)
    return json.loads(data.decode("utf-8"))

def testar_token(nome, caminho_json):
    print(f"\n=== {nome} ===")
    if not caminho_json.exists():
        print(f"❌ Arquivo não encontrado: {caminho_json}")
        return

    with open(caminho_json, "r", encoding="utf-8") as f:
        tok = json.load(f)

    access_token = tok.get("access_token")
    if not access_token:
        print("❌ access_token ausente no JSON.")
        return

    # Decodifica o JWT para pegar exp (quando expira) e e-mail
    try:
        claims = decode_jwt_payload(access_token)
        exp = claims.get("exp")
        email = claims.get("email")
    except Exception as e:
        print(f"⚠️ Não consegui decodificar o JWT: {e}")
        exp = None
        email = None

    # Tenta chamar o /userinfo
    headers = {"Authorization": f"Bearer {access_token}"}
    try:
        r = requests.get(USERINFO_URL, headers=headers, timeout=20)
    except Exception as e:
        print(f"❌ Erro de rede: {e}")
        return

    if r.status_code == 200:
        js = r.json()
        print("✅ Token VÁLIDO (userinfo OK)")
        print(f"   email: {js.get('email') or email}")
    else:
        print(f"❌ userinfo retornou {r.status_code}")
        try:
            print("   corpo:", r.json())
        except:
            print("   corpo:", r.text)

    # Mostra quanto tempo falta para expirar
    if exp:
        agora = int(time.time())
        faltam = exp - agora
        if faltam > 0:
            horas = faltam // 3600
            mins  = (faltam % 3600) // 60
            print(f"⏳ expira em ~ {horas}h {mins}m (epoch exp={exp})")
        else:
            print("⛔ access_token JÁ EXPIROU")
    else:
        print("⚠️ Não consegui calcular expiração (sem 'exp' no token).")

def main():
    for nome, caminho in TOKENS:
        testar_token(nome, caminho)

if __name__ == "__main__":
    main()
