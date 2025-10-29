# orion/integracoes/tiny_v3.py
# Helpers Tiny v3 — sem dependência do restante do app (exceto st.secrets para refresh)

import os
import json
import time
from typing import Optional, Tuple

import requests
import streamlit as st

_TINY_V3_BASE = "https://erp.tiny.com.br/public-api/v3"

# -------------------- Token em disco --------------------

def _token_path(emp: str) -> str:
    os.makedirs("tokens", exist_ok=True)
    return os.path.join("tokens", f"tiny_{emp}.json")

def _save_access_token(emp: str, access_token: str, expires_in: int | None = None):
    data = {"access_token": access_token}
    if expires_in:
        data["expires_in"] = expires_in
        data["saved_at"] = int(time.time())
    with open(_token_path(emp), "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def _load_access_token(emp: str) -> str | None:
    p = _token_path(emp)
    if not os.path.exists(p):
        return None
    try:
        with open(p, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return None

    tok = data.get("access_token")
    exp = data.get("expires_in")
    saved = data.get("saved_at")
    if tok and exp and saved:
        try:
            exp = int(exp); saved = int(saved)
            if time.time() >= saved + exp - 60:
                return None  # expira em breve → força refresh
        except Exception:
            pass
    return tok

def tiny_refresh_from_secrets(emp: str) -> str:
    sec_key = f"TINY_{emp.upper()}"
    if sec_key not in st.secrets:
        raise RuntimeError(
            f"Secrets '{sec_key}' não configurado. Em Settings → Secrets adicionar:\n"
            f"[{sec_key}]\nclient_id=\"...\"\nclient_secret=\"...\"\nrefresh_token=\"...\""
        )
    s = st.secrets[sec_key]
    payload = {
        "grant_type": "refresh_token",
        "client_id": s["client_id"],
        "client_secret": s["client_secret"],
        "refresh_token": s["refresh_token"],
    }
    r = requests.post(
        "https://accounts.tiny.com.br/realms/tiny/protocol/openid-connect/token",
        data=payload, timeout=30
    )
    if not r.ok:
        raise RuntimeError(f"Falha ao renovar token ({emp}): {r.status_code} {r.text[:200]}")
    data = r.json()
    access_token = data.get("access_token")
    if not access_token:
        raise RuntimeError("Tiny não retornou access_token.")
    _save_access_token(emp, access_token, data.get("expires_in"))
    return access_token

def tiny_get_bearer(emp: str) -> str:
    tok = _load_access_token(emp)
    if tok:
        return tok
    return tiny_refresh_from_secrets(emp)

# -------------------- Requests genéricos --------------------

def _req(token: str, method: str, path: str, params=None):
    url = f"{_TINY_V3_BASE}/{path.lstrip('/')}"
    headers = {"Authorization": f"Bearer {token}"}
    for attempt in range(3):
        r = requests.request(method, url, params=params, headers=headers, timeout=30)
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(1.2 * (attempt + 1)); continue
        if r.status_code == 401:
            raise RuntimeError("401 Unauthorized (access_token inválido/expirado).")
        if not r.ok:
            raise RuntimeError(f"HTTP {r.status_code} em {path}: {r.text[:300]}")
        try:
            return r.json()
        except Exception:
            raise RuntimeError(f"Resposta não-JSON em {path}: {r.text[:300]}")
    raise RuntimeError(f"Falha repetida em {path}")

# -------------------- Utilitários para variação/preço --------------------

def _node_get_code(node) -> Optional[str]:
    if isinstance(node, dict):
        for k in ('codigo','sku','codigo_sku','codigo_item','codigo_interno'):
            v = node.get(k)
            if v is not None and str(v).strip() != "":
                return str(v).strip().upper()
    return None

def _node_get_id(node) -> Optional[int]:
    if isinstance(node, dict):
        for k in ('id','idVariacao','id_variacao','idProduto'):
            v = node.get(k)
            if v not in (None, "", []):
                try:
                    return int(v)
                except:
                    pass
    return None

def _br_to_float(x):
    # versão interna para preco; não interfere no ETL do app
    if x is None:
        return None
    s = str(x).strip().replace("\u00a0"," ").replace("R$","").replace(" ","").replace(".","").replace(",",".")
    try:
        return float(s)
    except Exception:
        return None

def _search_price_in_node(node) -> Optional[float]:
    if not isinstance(node, dict):
        return None
    priorities = [
        'precoCustoMedio','preco_custo_medio','custoMedio','custo_medio',
        'precoCusto','preco_custo','precoCompra','preco_compra',
        'precoMedio','preco_medio',
        'preco','precoVenda','preco_venda',
        'precoPromocional','preco_promocional'
    ]
    spaces = [node]
    for key in ('precos','data'):
        if key in node and isinstance(node[key], dict):
            spaces.append(node[key])
    for space in spaces:
        for k in priorities:
            if k in space and space[k] not in (None, "", []):
                val = _br_to_float(space[k])
                if val is not None:
                    return val
    return None

def _scan_find_sku(node, skuU: str, result: dict):
    if isinstance(node, dict):
        code = _node_get_code(node)
        if code == skuU:
            if 'id' not in result or result['id'] is None:
                result['id'] = _node_get_id(node)
            if 'ean' not in result or not result.get('ean'):
                e = node.get('ean') or node.get('gtin')
                result['ean'] = str(e) if e not in (None,"") else ""
            result['price_node'] = node
        for v in node.values():
            _scan_find_sku(v, skuU, result)
    elif isinstance(node, list):
        for e in node:
            _scan_find_sku(e, skuU, result)

# -------------------- APIs Tiny: produto/variação/estoque --------------------

def tiny_resolve_variacao_por_sku(token: str, sku: str) -> Tuple[Optional[int], Optional[int], Optional[str], Optional[float]]:
    skuU = (sku or "").strip().upper()
    if not skuU:
        return None, None, None, None

    data = _req(token, "GET", "/produtos", params={"codigo": skuU})
    itens = data.get("itens") or data.get("items") or data.get("data") or []
    if not itens:
        return None, None, None, None
    pai_id = itens[0].get("id")
    if not pai_id:
        return None, None, None, None
    pai_id = int(pai_id)

    det = _req(token, "GET", f"/produtos/{pai_id}")
    found = {}
    _scan_find_sku(det, skuU, found)
    if found.get('id'):
        preco = _search_price_in_node(found.get('price_node', {}))
        return pai_id, int(found['id']), (found.get('ean') or ""), preco

    try:
        v = _req(token, "GET", f"/produtos/{pai_id}/variacoes")
        for key in ('variacoes','items','data','dados'):
            lst = v.get(key)
            if not lst:
                continue
            for n in lst:
                if _node_get_code(n) == skuU:
                    vid = _node_get_id(n)
                    ean = n.get('ean') or n.get('gtin') or ""
                    preco = _search_price_in_node(n)
                    return pai_id, (int(vid) if vid else None), str(ean), preco
    except Exception:
        pass

    return pai_id, None, None, None

def _is_considerado_deposito(d) -> bool:
    v = d.get("desconsiderar")
    return str(v).strip().upper() not in {"S", "TRUE", "1"}

def tiny_get_estoque_geral(token: str, produto_ou_variacao_id: int) -> dict | None:
    data = _req(token, "GET", f"/estoque/{produto_ou_variacao_id}")
    depositos = data.get("depositos") or data.get("data", {}).get("depositos") or data.get("data", {}).get("deposito") or []
    if not isinstance(depositos, list):
        depositos = [depositos] if depositos else []
    pref = None
    for d in depositos:
        nome = (d.get("nome") or "").strip().lower()
        if nome == "geral":
            pref = d; break
    if not pref and depositos:
        pref = next((d for d in depositos if _is_considerado_deposito(d)), depositos[0])
    if not pref:
        return None

    saldo = int(pref.get("saldo") or 0)
    reservado = int(pref.get("reservado") or 0)
    disponivel = int(pref.get("disponivel") or (saldo - reservado))
    return {"deposito_nome": pref.get("nome") or "", "saldo": saldo, "reservado": reservado, "disponivel": disponivel}
