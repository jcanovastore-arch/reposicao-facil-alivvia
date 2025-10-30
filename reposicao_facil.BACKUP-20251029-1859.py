# Reposição Logística — Alivvia (ORION Release 1 + ATHENAS payload OC)
# - Estrutura limpa
# - Sem mudança de comportamento no cálculo
# - Preparado para refactor futuro (ORION), mas funciona sozinho
# - ATHENAS: adiciona colunas novas ao payload da Ordem de Compra
#   (previsao_chegada, conf_recebimento_ok, qtde_recebida)
#
# Dependência opcional:
#   - orion.ui.components: bloco_filtros_e_selecao, preparar_df_para_oc
#     (se ausente, usamos um fallback interno simples para preparar_df_para_oc)

import io, os, re, json, time, hashlib
import datetime as dt
from typing import Optional, Tuple
import numpy as np
import pandas as pd
import streamlit as st
import requests
from requests.adapters import HTTPAdapter, Retry
from unidecode import unidecode

# =========================
#  Versão do App
# =========================
VERSION = "ORION-r1 (v3.4.0)"

# =========================
#  Imports opcionais da UI (ORION)
# =========================
try:
    from orion.ui.components import bloco_filtros_e_selecao, preparar_df_para_oc as preparar_df_para_oc_ext
except Exception:
    bloco_filtros_e_selecao = None
    preparar_df_para_oc_ext = None

# =========================
#  Helpers gerais
# =========================
def norm_header(s: str) -> str:
    s = (s or "").strip()
    s = unidecode(s).lower()
    for ch in [" ", "-", "(", ")", "/", "\\", "[", "]", ".", ",", ";", ":"]:
        s = s.replace(ch, "_")
    while "__" in s:
        s = s.replace("__", "_")
    return s.strip("_")

def normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [norm_header(c) for c in df.columns]
    return df

def norm_sku(x: str) -> str:
    if pd.isna(x):
        return ""
    return unidecode(str(x)).strip().upper()

def br_to_float(x):
    if pd.isna(x):
        return np.nan
    if isinstance(x, (int, float, np.integer, np.floating)):
        return float(x)
    s = str(x).strip()
    s = s.replace("\u00a0", " ").replace("R$", "").replace(" ", "").replace(".", "").replace(",", ".")
    try:
        return float(s)
    except:
        return np.nan

# =========================
#  HTTP básico (Sheets)
# =========================
def _requests_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(total=3, backoff_factor=0.6, status_forcelist=[429, 500, 502, 503, 504], allowed_methods=frozenset(["GET"]))
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.headers.update({"User-Agent": "Mozilla/5.0"})
    return s

def gs_export_xlsx_url(sheet_id: str) -> str:
    return f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=xlsx"

def baixar_xlsx_do_sheets(sheet_id: str) -> bytes:
    s = _requests_session()
    r = s.get(gs_export_xlsx_url(sheet_id), timeout=30)
    r.raise_for_status()
    return r.content

# =========================
#  Persistência simples de uploads (disco + cache)
# =========================
BASE_UPLOAD_DIR = ".uploads"

@st.cache_resource(show_spinner=False)
def _file_store():
    return {
        "ALIVVIA": {"FULL": None, "VENDAS": None, "ESTOQUE": None},
        "JCA":     {"FULL": None, "VENDAS": None, "ESTOQUE": None},
    }

def _disk_dir(emp: str, kind: str) -> str:
    p = os.path.join(BASE_UPLOAD_DIR, emp, kind)
    os.makedirs(p, exist_ok=True)
    return p

def _disk_put(emp: str, kind: str, name: str, blob: bytes):
    p = _disk_dir(emp, kind)
    with open(os.path.join(p, "file.bin"), "wb") as f:
        f.write(blob)
    with open(os.path.join(p, "meta.json"), "w", encoding="utf-8") as f:
        json.dump({"name": name}, f)

def _disk_get(emp: str, kind: str):
    p = _disk_dir(emp, kind)
    meta = os.path.join(p, "meta.json")
    data = os.path.join(p, "file.bin")
    if not (os.path.exists(meta) and os.path.exists(data)):
        return None
    try:
        with open(meta, "r", encoding="utf-8") as f:
            info = json.load(f)
        with open(data, "rb") as f:
            blob = f.read()
        return {"name": info.get("name", "arquivo.bin"), "bytes": blob}
    except Exception:
        return None

def _store_put(emp: str, kind: str, name: str, blob: bytes):
    store = _file_store()
    store[emp][kind] = {"name": name, "bytes": blob}
    _disk_put(emp, kind, name, blob)

def _store_get(emp: str, kind: str):
    store = _file_store()
    it = store[emp][kind]
    if it:
        return it
    it = _disk_get(emp, kind)
    if it:
        store[emp][kind] = it
        return it
    return None

def _store_clear(emp: str):
    store = _file_store()
    store[emp] = {"FULL": None, "VENDAS": None, "ESTOQUE": None}
    p = os.path.join(BASE_UPLOAD_DIR, emp)
    if os.path.isdir(p):
        for root, _, files in os.walk(p):
            for fn in files:
                try:
                    os.remove(os.path.join(root, fn))
                except:
                    pass

def badge_ok(label: str, filename: str) -> str:
    return f"<span style='background:#198754; color:#fff; padding:6px 10px; border-radius:10px; font-size:12px;'>✅ {label}: <b>{filename}</b></span>"

# =========================
#  TINY v3 — Helpers
# =========================
_TINY_V3_BASE = "https://erp.tiny.com.br/public-api/v3"

def _tiny_v3_token_path(emp: str) -> str:
    os.makedirs("tokens", exist_ok=True)
    return os.path.join("tokens", f"tiny_{emp}.json")

def _tiny_v3_save_access_token(emp: str, access_token: str, expires_in: int | None = None):
    data = {"access_token": access_token}
    if expires_in:
        data["expires_in"] = expires_in
        data["saved_at"] = int(time.time())
    with open(_tiny_v3_token_path(emp), "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def _tiny_v3_load_access_token(emp: str) -> str | None:
    p = _tiny_v3_token_path(emp)
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
                return None  # força refresh
        except Exception:
            pass
    return tok

def _tiny_v3_refresh_from_secrets(emp: str) -> str:
    sec_key = f"TINY_{emp.upper()}"
    if sec_key not in st.secrets:
        raise RuntimeError(
            f"Secrets '{sec_key}' não configurado. Vá em Manage app → Settings → Secrets e adicione:\n"
            f"[{sec_key}]\nclient_id=\"...\"\nclient_secret=\"...\"\nrefresh_token=\"...\""
        )
    s = st.secrets[sec_key]
    payload = {"grant_type": "refresh_token","client_id": s["client_id"],"client_secret": s["client_secret"],"refresh_token": s["refresh_token"]}
    r = requests.post("https://accounts.tiny.com.br/realms/tiny/protocol/openid-connect/token", data=payload, timeout=30)
    if not r.ok:
        raise RuntimeError(f"Falha ao renovar token ({emp}): {r.status_code} {r.text[:200]}")
    data = r.json()
    access_token = data.get("access_token")
    if not access_token:
        raise RuntimeError("Tiny não retornou access_token.")
    _tiny_v3_save_access_token(emp, access_token, data.get("expires_in"))
    return access_token

def _tiny_v3_get_bearer(emp: str) -> str:
    tok = _tiny_v3_load_access_token(emp)
    if tok:
        return tok
    return _tiny_v3_refresh_from_secrets(emp)

def _tiny_v3_req(token: str, method: str, path: str, params=None):
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
                try: return int(v)
                except: pass
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
            if k in space and space[k] not in (None,"",[]):
                try: return br_to_float(space[k])
                except: pass
    return None

def _scan_find_sku(node, skuU: str, result: dict):
    if isinstance(node, dict):
        code = _node_get_code(node)
        if code == skuU:
            if 'id' not in result or result['id'] is None:
                result['id'] = _node_get_id(node)
            if 'ean' not in result or not result['ean']:
                e = node.get('ean') or node.get('gtin'); result['ean'] = str(e) if e not in (None,"") else ""
            result['price_node'] = node
        for v in node.values():
            _scan_find_sku(v, skuU, result)
    elif isinstance(node, list):
        for e in node:
            _scan_find_sku(e, skuU, result)

def _tiny_v3_resolve_variacao_por_sku(token: str, sku: str) -> Tuple[Optional[int], Optional[int], Optional[str], Optional[float]]:
    skuU = (sku or "").strip().upper()
    if not skuU: return None, None, None, None
    data = _tiny_v3_req(token, "GET", "/produtos", params={"codigo": skuU})
    itens = data.get("itens") or data.get("items") or data.get("data") or []
    if not itens: return None, None, None, None
    pai_id = itens[0].get("id")
    if not pai_id: return None, None, None, None
    pai_id = int(pai_id)
    det = _tiny_v3_req(token, "GET", f"/produtos/{pai_id}")
    found = {}; _scan_find_sku(det, skuU, found)
    if found.get('id'):
        preco = _search_price_in_node(found.get('price_node', {}))
        return pai_id, int(found['id']), (found.get('ean') or ""), preco
    try:
        v = _tiny_v3_req(token, "GET", f"/produtos/{pai_id}/variacoes")
        for key in ('variacoes','items','data','dados'):
            lst = v.get(key)
            if not lst: continue
            for n in lst:
                if _node_get_code(n) == skuU:
                    vid = _node_get_id(n); ean = n.get('ean') or n.get('gtin') or ""; preco = _search_price_in_node(n)
                    return pai_id, (int(vid) if vid else None), str(ean), preco
    except Exception:
        pass
    return pai_id, None, None, None

def _is_considerado_deposito(d) -> bool:
    v = d.get("desconsiderar")
    return str(v).strip().upper() not in {"S", "TRUE", "1"}

def _tiny_v3_get_estoque_geral(token: str, produto_ou_variacao_id: int) -> dict | None:
    data = _tiny_v3_req(token, "GET", f"/estoque/{produto_ou_variacao_id}")
    depositos = data.get("depositos") or data.get("data", {}).get("depositos") or data.get("data", {}).get("deposito") or []
    if not isinstance(depositos, list): depositos = [depositos] if depositos else []
    pref = None
    for d in depositos:
        nome = (d.get("nome") or "").strip().lower()
        if nome == "geral": pref = d; break
    if not pref and depositos:
        pref = next((d for d in depositos if _is_considerado_deposito(d)), depositos[0])
    if not pref: return None
    saldo = int(pref.get("saldo") or 0); reservado = int(pref.get("reservado") or 0)
    disponivel = int(pref.get("disponivel") or (saldo - reservado))
    return {"deposito_nome": pref.get("nome") or "", "saldo": saldo, "reservado": reservado, "disponivel": disponivel}

def _carregar_skus_base(emp: str) -> list[str]:
    # 1) se já tem catálogo no estado
    try:
        dfc = st.session_state.get("catalogo_df")
        if dfc is not None and isinstance(dfc, pd.DataFrame) and not dfc.empty:
            col = None
            for c in ("sku","component_sku","codigo","codigo_sku"):
                if c in dfc.columns:
                    col = c; break
            if col:
                skus = dfc[col].dropna().astype(str).str.strip().str.upper().unique().tolist()
                if skus: return skus
    except Exception:
        pass
    # 2) tenta .uploads/EMP
    base = os.path.join(".uploads", emp.upper())
    if os.path.isdir(base):
        for root, _, files in os.walk(base):
            cand = [f for f in files if any(k in f.upper() for k in ("PADRAO","KITS","CAT")) and f.lower().endswith((".csv",".xlsx"))]
            cand.sort(reverse=True)
            if cand:
                path = os.path.join(root, cand[0])
                try:
                    if path.lower().endswith(".xlsx"):
                        df = pd.read_excel(path, dtype=str, keep_default_na=False)
                    else:
                        df = pd.read_csv(path, dtype=str, keep_default_na=False, sep=None, engine="python")
                    for col in ("SKU","sku","component_sku","codigo","codigo_sku"):
                        if col in df.columns:
                            skus = df[col].dropna().astype(str).str.strip().str.upper().unique().tolist()
                            if skus: return skus
                except Exception:
                    pass
    return []

def sincronizar_estoque_tiny(emp: str, skus: list[str]) -> pd.DataFrame:
    token = _tiny_v3_get_bearer(emp)
    linhas = []; total = len(skus)
    prog = st.progress(0, text=f"Sincronizando {total} SKUs no Tiny ({emp})…")
    for i, sku in enumerate(skus, start=1):
        skuU = (sku or "").strip().upper()
        try:
            try:
                pai_id, var_id, ean, preco = _tiny_v3_resolve_variacao_por_sku(token, skuU)
            except RuntimeError as e:
                if "401" in str(e):
                    token = _tiny_v3_refresh_from_secrets(emp)
                    pai_id, var_id, ean, preco = _tiny_v3_resolve_variacao_por_sku(token, skuU)
                else:
                    raise
            if not pai_id:
                linhas.append({"SKU": skuU, "Estoque_Fisico": 0, "Preco": 0.0, "status": "SKU não encontrado"})
            elif not var_id:
                linhas.append({"SKU": skuU, "Estoque_Fisico": 0, "Preco": float(preco or 0.0), "status": "SKU do PAI (sem variação)"})
            else:
                try:
                    est = _tiny_v3_get_estoque_geral(token, var_id)
                except RuntimeError as e:
                    if "401" in str(e):
                        token = _tiny_v3_refresh_from_secrets(emp)
