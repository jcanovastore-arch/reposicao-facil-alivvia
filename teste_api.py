# teste_api.py
import json
from tiny_api import bearer_headers, http_post

USERINFO_URL = "https://accounts.tiny.com.br/realms/tiny/protocol/openid-connect/userinfo"

def show(title, obj):
    print(f"\n==== {title} ====")
    print(json.dumps(obj, indent=2, ensure_ascii=False))

def testar_conta(conta: str):
    h = bearer_headers(conta)           # monta Authorization: Bearer <token>
    js = http_post(USERINFO_URL, h, {}) # chama userinfo
    show(f"{conta} - userinfo (token válido)", js)

if __name__ == "__main__":
    testar_conta("JCA")
    testar_conta("ALIVVIA")
