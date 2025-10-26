# auth_helper_multi.py
# Garante access_token válido (com refresh automático) para cada conta (JCA/ALIVVIA)
import os, json, time, base64, requests
from typing import Dict

# ====== PREENCHA os Client IDs/Secrets de cada conta ======
# (são os MESMOS que você usou para gerar os tokens)
CLIENTS: Dict[str, Dict[str, str]] = {
    "JCA": {
        "client_id":     "tiny-api-0f4c1f7383e30bde76b72b3b94ad78a8ceb4cb70-1761414928",
        "client_secret": "yaIipiTUDdeSlsy43DKwox2koH6YvQVq",
    },
    "ALIVVIA": {
        "client_id":     "tiny-api-f6f6bb60d310a378fc0d68952963ef2408ad15b7-1761425352",
        "client_secret": "ZyUhWnM9XvQxHKGUDCp8cUoGyn3Hk3ym",
    },
}
# ===========================================================

TOKEN_URL = "https://accounts.tiny.com.br/realms/tiny/protocol/openid-connect/token"
TOKEN_FILE_TEMPLATE = "tokens_tiny_{conta}.json"  # ex.: tokens_tiny_JCA.json

def _token_file(conta: str) -> str:
    return TOKEN_FILE_TEMPLATE.format(conta=conta.upper())

def _load_tokens(conta: str) -> dict:
    fn = _token_file(conta)
    if not os.path.exists(fn):
        raise RuntimeError(f"[{conta}] Arquivo de tokens não encontrado: {fn}. Gere com tiny_connect_v3.py.")
    with open(fn, "r", encoding="utf-8") as f:
        t = json.load(f)
    # primeira vez: grava timestamp para controlar expiração
    if "obtido_em" not in t:
        t["obtido_em"] = int(time.time())
        _save_tokens(conta, t)
    return t

def _save_tokens(conta: str, js: dict):
    js.setdefault("obtido_em", int(time.time()))
    with open(_token_file(conta), "w", encoding="utf-8") as f:
        json.dump(js, f, indent=2, ensure_ascii=False)

def _refresh_access_token(conta: str, t: dict) -> str:
    cfg = CLIENTS.get(conta.upper())
    if not cfg or not cfg.get("client_id") or not cfg.get("client_secret"):
        raise RuntimeError(f"[{conta}] Client ID/Secret não configurados em CLIENTS no auth_helper_multi.py.")

    cid = cfg["client_id"]
    csc = cfg["client_secret"]
    if "refresh_token" not in t:
        raise RuntimeError(f"[{conta}] refresh_token ausente no arquivo de tokens.")

    auth = base64.b64encode(f"{cid}:{csc}".encode()).decode()
    headers = {"Authorization": f"Basic {auth}", "Content-Type": "application/x-www-form-urlencoded"}
    data = {"grant_type": "refresh_token", "refresh_token": t["refresh_token"]}

    r = requests.post(TOKEN_URL, headers=headers, data=data, timeout=30)
    r.raise_for_status()
    js = r.json()
    if "access_token" not in js:
        raise RuntimeError(f"[{conta}] Falha ao renovar token: {js}")

    js["obtido_em"] = int(time.time())
    _save_tokens(conta, js)
    return js["access_token"]

def get_access_token(conta: str) -> str:
    """
    Retorna um access_token válido para a conta (JCA/ALIVVIA).
    Se estiver a ~5min de expirar, faz refresh automaticamente e salva no arquivo.
    """
    conta = conta.upper()
    t = _load_tokens(conta)
    expira_em = t["obtido_em"] + int(t.get("expires_in", 3600))
    agora = int(time.time())

    # renova se faltarem < 5 minutos
    if agora > expira_em - 300:
        return _refresh_access_token(conta, t)
    return t["access_token"]
