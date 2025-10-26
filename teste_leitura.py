# teste_leitura.py
from tiny_api import get_userinfo

def imprime(conta: str):
    try:
        js = get_userinfo(conta)
        email = js.get("email", "-")
        print(f"=== {conta} ===")
        print("✅ token OK | email:", email)
    except Exception as e:
        print(f"=== {conta} ===")
        print("❌ falhou:", e)

if __name__ == "__main__":
    imprime("JCA")
    imprime("ALIVVIA")
