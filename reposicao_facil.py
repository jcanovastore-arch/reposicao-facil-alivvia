# ReposiÃ§Ã£o LogÃ­stica â€” Alivvia
# MantÃ©m as abas: Dados das Empresas, Compra AutomÃ¡tica, AlocaÃ§Ã£o de Compra
# Inclui: filtros inteligentes, seleÃ§Ã£o persistente p/ OC, Tiny v3, alocaÃ§Ã£o proporcional 60d

import io
import os
import re
import json
import time
import hashlib
import datetime as dt
from dataclasses import dataclass
from typing import Optional, Tuple
from orion.ui.components import bloco_filtros_e_selecao, preparar_df_para_oc
from orion.integracoes.tiny_v3 import (
    tiny_get_bearer,
    tiny_refresh_from_secrets,
    tiny_resolve_variacao_por_sku,
    tiny_get_estoque_geral,
)
from orion.dominio.padrao import Catalogo, carregar_padrao_do_xlsx, _carregar_padrao_de_content



import numpy as np
import pandas as pd
import streamlit as st
import requests
from requests.adapters import HTTPAdapter, Retry
from unidecode import unidecode

# =========================
#  VersÃ£o do App
# =========================
VERSION = "v3.3.4"

# =========================
#  TINY v3 â€” Helpers
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
                return None  # forÃ§a refresh
        except Exception:
            pass
    return tok

def _tiny_v3_refresh_from_secrets(emp: str) -> str:
    sec_key = f"TINY_{emp.upper()}"
    if sec_key not in st.secrets:
        raise RuntimeError(
            f"Secrets '{sec_key}' nÃ£o configurado. VÃ¡ em Manage app â†’ Settings â†’ Secrets e adicione:\n"
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
        raise RuntimeError("Tiny nÃ£o retornou access_token.")
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
            time.sleep(1.2 * (attempt + 1))
            continue
        if r.status_code == 401:
            raise RuntimeError("401 Unauthorized (access_token invÃ¡lido/expirado).")
        if not r.ok:
            raise RuntimeError(f"HTTP {r.status_code} em {path}: {r.text[:300]}")
        try:
            return r.json()
        except Exception:
            raise RuntimeError(f"Resposta nÃ£o-JSON em {path}: {r.text[:300]}")
    raise RuntimeError(f"Falha repetida em {path}")

# ----------- UtilitÃ¡rios p/ achar VariaÃ§Ã£o + PreÃ§o -----------

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
    for key in ('precos', 'data'):
        if key in node and isinstance(node[key], dict):
            spaces.append(node[key])
    for space in spaces:
        for k in priorities:
            if k in space and space[k] not in (None, "", []):
                try:
                    return br_to_float(space[k])
                except Exception:
                    pass
    return None

def _scan_find_sku(node, skuU: str, result: dict):
    if isinstance(node, dict):
        code = _node_get_code(node)
        if code == skuU:
            if 'id' not in result or result['id'] is None:
                result['id'] = _node_get_id(node)
            if 'ean' not in result or not result['ean']:
                e = node.get('ean') or node.get('gtin')
                result['ean'] = str(e) if e not in (None,"") else ""
            result['price_node'] = node
        for v in node.values():
            _scan_find_sku(v, skuU, result)
    elif isinstance(node, list):
        for e in node:
            _scan_find_sku(e, skuU, result)

def _tiny_v3_resolve_variacao_por_sku(token: str, sku: str) -> Tuple[Optional[int], Optional[int], Optional[str], Optional[float]]:
    skuU = (sku or "").strip().upper()
    if not skuU:
        return None, None, None, None

    data = _tiny_v3_req(token, "GET", "/produtos", params={"codigo": skuU})
    itens = data.get("itens") or data.get("items") or data.get("data") or []
    if not itens:
        return None, None, None, None
    pai_id = itens[0].get("id")
    if not pai_id:
        return None, None, None, None
    pai_id = int(pai_id)

    det = _tiny_v3_req(token, "GET", f"/produtos/{pai_id}")
    found = {}
    _scan_find_sku(det, skuU, found)
    if found.get('id'):
        preco = _search_price_in_node(found.get('price_node', {}))
        return pai_id, int(found['id']), (found.get('ean') or ""), preco

    try:
        v = _tiny_v3_req(token, "GET", f"/produtos/{pai_id}/variacoes")
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

def _tiny_v3_get_estoque_geral(token: str, produto_ou_variacao_id: int) -> dict | None:
    data = _tiny_v3_req(token, "GET", f"/estoque/{produto_ou_variacao_id}")
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

def _carregar_skus_base(emp: str) -> list[str]:
    try:
        dfc = st.session_state.get("catalogo_df")
        if dfc is not None and isinstance(dfc, pd.DataFrame) and not dfc.empty:
            col = None
            for c in ("sku", "component_sku", "codigo", "codigo_sku"):
                if c in dfc.columns:
                    col = c; break
            if col:
                skus = (
                    dfc[col].dropna().astype(str).str.strip().str.upper().unique().tolist()
                )
                if skus:
                    return skus
    except Exception:
        pass

    base = os.path.join(".uploads", emp.upper())
    if os.path.isdir(base):
        for root, _, files in os.walk(base):
            cand = [
                f for f in files
                if any(k in f.upper() for k in ("PADRAO", "KITS", "CAT"))
                and f.lower().endswith((".csv", ".xlsx"))
            ]
            cand.sort(reverse=True)
            if cand:
                path = os.path.join(root, cand[0])
                try:
                    if path.lower().endswith(".xlsx"):
                        df = pd.read_excel(path, dtype=str, keep_default_na=False)
                    else:
                        df = pd.read_csv(path, dtype=str, keep_default_na=False, sep=None, engine="python")
                    for col in ("SKU", "sku", "component_sku", "codigo", "codigo_sku"):
                        if col in df.columns:
                            skus = (
                                df[col].dropna().astype(str).str.strip().str.upper().unique().tolist()
                            )
                            if skus:
                                return skus
                except Exception:
                    pass
    return []

def sincronizar_estoque_tiny(emp: str, skus: list[str]) -> pd.DataFrame:
    token = _tiny_v3_get_bearer(emp)
    linhas = []
    total = len(skus)
    prog = st.progress(0, text=f"Sincronizando {total} SKUs no Tiny ({emp})â€¦")
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
                linhas.append({"SKU": skuU, "Estoque_Fisico": 0, "Preco": 0.0, "status": "SKU nÃ£o encontrado"})
            elif not var_id:
                linhas.append({"SKU": skuU, "Estoque_Fisico": 0, "Preco": float(preco or 0.0), "status": "SKU do PAI (sem variaÃ§Ã£o)"})
            else:
                try:
                    est = _tiny_v3_get_estoque_geral(token, var_id)
                except RuntimeError as e:
                    if "401" in str(e):
                        token = _tiny_v3_refresh_from_secrets(emp)
                        est = _tiny_v3_get_estoque_geral(token, var_id)
                    else:
                        raise
                dispo = int(est["disponivel"]) if est else 0
                if preco is None:
                    try:
                        det_var = _tiny_v3_req(token, "GET", f"/produtos/{var_id}")
                        preco = _search_price_in_node(det_var)
                    except Exception:
                        preco = None
                linhas.append({"SKU": skuU, "Estoque_Fisico": dispo, "Preco": float(preco or 0.0), "status": "OK"})
        except Exception as e:
            linhas.append({"SKU": skuU, "Estoque_Fisico": 0, "Preco": 0.0, "status": f"ERRO: {e}"})
        if total:
            prog.progress(min(i/total, 1.0), text=f"Sincronizando {i}/{total}â€¦")
    prog.empty()
    cols = ["SKU","Estoque_Fisico","Preco","status"]
    df = pd.DataFrame(linhas)[cols]
    df["Estoque_Fisico"] = pd.to_numeric(df["Estoque_Fisico"], errors="coerce").fillna(0).astype(int)
    df["Preco"] = pd.to_numeric(df["Preco"], errors="coerce").fillna(0.0)
    return df

# =========================
#  App base
# =========================

st.set_page_config(page_title="ReposiÃ§Ã£o LogÃ­stica â€” Alivvia", layout="wide")

DEFAULT_SHEET_LINK = (
    "https://docs.google.com/spreadsheets/d/1cTLARjq-B5g50dL6tcntg7lb_Iu0ta43/edit"
)
DEFAULT_SHEET_ID = "1cTLARjq-B5g50dL6tcntg7lb_Iu0ta43"

def _requests_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(total=3, backoff_factor=0.6, status_forcelist=[429, 500, 502, 503, 504], allowed_methods=frozenset(["GET"]))
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.headers.update({"User-Agent": "Mozilla/5.0"})
    return s

def gs_export_xlsx_url(sheet_id: str) -> str:
    return f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=xlsx"

def extract_sheet_id_from_url(url: str) -> Optional[str]:
    if not url:
        return None
    m = re.search(r"/d/([a-zA-Z0-9\-_]+)/", url)
    return m.group(1) if m else None

def baixar_xlsx_do_sheets(sheet_id: str) -> bytes:
    s = _requests_session()
    r = s.get(gs_export_xlsx_url(sheet_id), timeout=30)
    r.raise_for_status()
    return r.content

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

# ---------- PersistÃªncia de uploads ----------
BASE_UPLOAD_DIR = ".uploads"

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

@st.cache_resource(show_spinner=False)
def _file_store():
    return {
        "ALIVVIA": {"FULL": None, "VENDAS": None, "ESTOQUE": None},
        "JCA":     {"FULL": None, "VENDAS": None, "ESTOQUE": None},
    }

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
    return f"<span style='background:#198754; color:#fff; padding:6px 10px; border-radius:10px; font-size:12px;'>âœ… {label}: <b>{filename}</b></span>"

# ---------- Estado ----------
def _ensure_state():
    st.session_state.setdefault("catalogo_df", None)
    st.session_state.setdefault("kits_df", None)
    st.session_state.setdefault("loaded_at", None)
    st.session_state.setdefault("resultado_compra", {})
    st.session_state.setdefault("estoque_tiny_por_emp", {})
    st.session_state.setdefault("df_compra", None)
    st.session_state.setdefault("oc_selection", {"ALIVVIA": set(), "JCA": set()})
    for emp in ["ALIVVIA", "JCA"]:
        st.session_state.setdefault(emp, {})
        for kind in ["FULL", "VENDAS", "ESTOQUE"]:
            st.session_state[emp].setdefault(kind, {"name": None, "bytes": None})
            if st.session_state[emp][kind]["name"] is None:
                it = _store_get(emp, kind)
                if it:
                    st.session_state[emp][kind] = it
_ensure_state()

# ---------- Leitura arquivos ----------
def load_any_table_from_bytes(file_name: str, blob: bytes) -> pd.DataFrame:
    bio = io.BytesIO(blob)
    name = (file_name or "").lower()
    try:
        if name.endswith(".csv"):
            df = pd.read_csv(bio, dtype=str, keep_default_na=False, sep=None, engine="python")
        else:
            df = pd.read_excel(bio, dtype=str, keep_default_na=False)
    except Exception as e:
        raise RuntimeError(f"NÃ£o consegui ler '{file_name}': {e}")
    df.columns = [norm_header(c) for c in df.columns]
    if not any("sku" in c for c in df.columns):
        try:
            bio.seek(0)
            if name.endswith(".csv"):
                df = pd.read_csv(bio, dtype=str, keep_default_na=False, header=2)
            else:
                df = pd.read_excel(bio, dtype=str, keep_default_na=False, header=2)
            df.columns = [norm_header(c) for c in df.columns]
        except Exception:
            pass
    sku_col = next((c for c in ["sku", "codigo", "codigo_sku"] if c in df.columns), None)
    if sku_col:
        df[sku_col] = df[sku_col].map(norm_sku)
        df = df[df[sku_col] != ""]
    return df.reset_index(drop=True)

# ---------- PadrÃ£o KITS/CAT ----------
@dataclass
class Catalogo:
    catalogo_simples: pd.DataFrame  # component_sku, fornecedor, status_reposicao
    kits_reais: pd.DataFrame        # kit_sku, component_sku, qty

def _carregar_padrao_de_content(content: bytes) -> "Catalogo":
    xls = pd.ExcelFile(io.BytesIO(content))

    def load_sheet(opts):
        for n in opts:
            if n in xls.sheet_names:
                return pd.read_excel(xls, n, dtype=str, keep_default_na=False)
        raise RuntimeError(f"Aba nÃ£o encontrada. Esperado uma de {opts}. Abas: {xls.sheet_names}")

    df_kits = load_sheet(["KITS", "KITS_REAIS", "kits", "kits_reais"]).copy()
    df_cat = load_sheet(["CATALOGO_SIMPLES", "CATALOGO", "catalogo_simples", "catalogo"]).copy()

    # KITS
    df_kits = normalize_cols(df_kits)
    m = {}
    for alvo, cand in {
        "kit_sku": ["kit_sku", "kit", "sku_kit"],
        "component_sku": ["component_sku", "componente", "sku_componente", "component"],
        "qty": ["qty", "qtd", "quantidade", "qty_por_kit", "qtd_por_kit", "quantidade_por_kit"],
    }.items():
        for c in cand:
            if c in df_kits.columns:
                m[c] = alvo
                break
    df_kits = df_kits.rename(columns=m)
    for col in ["kit_sku", "component_sku", "qty"]:
        if col not in df_kits.columns:
            raise RuntimeError("KITS precisa de 'kit_sku', 'component_sku', 'qty'.")
    df_kits = df_kits[["kit_sku", "component_sku", "qty"]].copy()
    df_kits["kit_sku"] = df_kits["kit_sku"].map(norm_sku)
    df_kits["component_sku"] = df_kits["component_sku"].map(norm_sku)
    df_kits["qty"] = pd.to_numeric(df_kits["qty"], errors="coerce").fillna(0).astype(int)
    df_kits = df_kits[df_kits["qty"] >= 1].drop_duplicates(subset=["kit_sku", "component_sku"])

    # CATALOGO
    df_cat = normalize_cols(df_cat)
    m = {}
    for alvo, cand in {
        "component_sku": ["component_sku", "sku", "produto", "item", "codigo", "sku_componente"],
        "fornecedor": ["fornecedor", "supplier", "fab", "marca"],
        "status_reposicao": ["status_reposicao", "status", "reposicao_status"],
    }.items():
        for c in cand:
            if c in df_cat.columns:
                m[c] = alvo
                break
    df_cat = df_cat.rename(columns=m)
    if "component_sku" not in df_cat.columns:
        raise ValueError("CATALOGO precisa da coluna 'component_sku'.")
    if "fornecedor" not in df_cat.columns:
        df_cat["fornecedor"] = ""
    if "status_reposicao" not in df_cat.columns:
        df_cat["status_reposicao"] = ""
    df_cat["component_sku"] = df_cat["component_sku"].map(norm_sku)
    df_cat["fornecedor"] = df_cat["fornecedor"].fillna("")
    df_cat["status_reposicao"] = df_cat["status_reposicao"].fillna("")
    df_cat = df_cat.drop_duplicates(subset=["component_sku"], keep="last")

    return Catalogo(df_cat, df_kits)

def carregar_padrao_do_xlsx(sheet_id: str) -> Catalogo:
    content = baixar_xlsx_do_sheets(sheet_id)
    return _carregar_padrao_de_content(content)

# ---------- Mapear tipos/colunas ----------
def mapear_tipo(df: pd.DataFrame) -> str:
    cols = [c.lower() for c in df.columns]
    tem_sku = any("sku" in c for c in cols)
    tem_v60 = any(c.startswith("vendas_60d") or c in {"vendas 60d", "vendas_qtd_60d"} for c in cols)
    tem_estoque_full = any(("estoque" in c and "full" in c) or c == "estoque_full" for c in cols)
    tem_transito = any(("transito" in c) or c in {"em_transito", "em transito", "em_transito_full"} for c in cols)
    tem_estoque_generico = any(c in {"estoque_atual", "qtd", "quantidade"} or "estoque" in c for c in cols)
    tem_preco = any(c in {"preco", "preco_compra", "custo", "custo_medio", "preco_medio", "preco_unitario"} for c in cols)

    if tem_sku and (tem_v60 or tem_estoque_full or tem_transito):
        return "FULL"
    if tem_sku and tem_estoque_generico and tem_preco:
        return "FISICO"
    if tem_sku and not tem_preco:
        return "VENDAS"
    return "DESCONHECIDO"

def mapear_colunas(df: pd.DataFrame, tipo: str) -> pd.DataFrame:
    if tipo == "FULL":
        if "sku" in df.columns:
            df["SKU"] = df["sku"].map(norm_sku)
        elif "codigo" in df.columns:
            df["SKU"] = df["codigo"].map(norm_sku)
        elif "codigo_sku" in df.columns:
            df["SKU"] = df["codigo_sku"].map(norm_sku)
        else:
            raise RuntimeError("FULL invÃ¡lido: precisa de SKU/codigo.")
        c_v = [c for c in df.columns if c in ["vendas_qtd_60d", "vendas_60d", "vendas 60d"] or c.startswith("vendas_60d")]
        if not c_v:
            raise RuntimeError("FULL invÃ¡lido: faltou Vendas_60d.")
        df["Vendas_Qtd_60d"] = pd.to_numeric(df[c_v[0]].map(br_to_float), errors="coerce").fillna(0).astype(int)
        c_e = [c for c in df.columns if c in ["estoque_full", "estoque_atual"] or ("estoque" in c and "full" in c)]
        if not c_e:
            raise RuntimeError("FULL invÃ¡lido: faltou Estoque_Full.")
        df["Estoque_Full"] = pd.to_numeric(df[c_e[0]].map(br_to_float), errors="coerce").fillna(0).astype(int)
        c_t = [c for c in df.columns if c in ["em_transito", "em transito", "em_transito_full"] or ("transito" in c)]
        df["Em_Transito"] = pd.to_numeric(df[c_t[0]].map(br_to_float), errors="coerce").fillna(0).astype(int) if c_t else 0
        return df[["SKU", "Vendas_Qtd_60d", "Estoque_Full", "Em_Transito"]].copy()

    if tipo == "FISICO":
        sku_series = (
            df["sku"] if "sku" in df.columns else
            (df["codigo"] if "codigo" in df.columns else
             (df["codigo_sku"] if "codigo_sku" in df.columns else None))
        )
        if sku_series is None:
            cand = next((c for c in df.columns if "sku" in c.lower()), None)
            if cand is None:
                raise RuntimeError("FÃSICO invÃ¡lido: nÃ£o achei SKU.")
            sku_series = df[cand]
        df["SKU"] = sku_series.map(norm_sku)
        c_q = [c for c in df.columns if c in ["estoque_atual", "qtd", "quantidade"] or ("estoque" in c)]
        if not c_q:
            raise RuntimeError("FÃSICO invÃ¡lido: faltou Estoque.")
        df["Estoque_Fisico"] = pd.to_numeric(df[c_q[0]].map(br_to_float), errors="coerce").fillna(0).astype(int)
        c_p = [c for c in df.columns if c in ["preco", "preco_compra", "custo", "custo_medio", "preco_medio", "preco_unitario"]]
        if not c_p:
            raise RuntimeError("FÃSICO invÃ¡lido: faltou PreÃ§o/Custo.")
        df["Preco"] = pd.to_numeric(df[c_p[0]].map(br_to_float), errors="coerce").fillna(0.0)
        return df[["SKU", "Estoque_Fisico", "Preco"]].copy()

    if tipo == "VENDAS":
        sku_col = next((c for c in df.columns if "sku" in c.lower()), None)
        if not sku_col:
            raise RuntimeError("VENDAS invÃ¡lido: nÃ£o achei SKU.")
        df["SKU"] = df[sku_col].map(norm_sku)
        cand_qty = []
        for c in df.columns:
            cl = c.lower()
            score = 0
            if "qtde" in cl: score += 3
            if "quant" in cl: score += 2
            if "venda" in cl: score += 1
            if "order" in cl: score += 1
            if score > 0:
                cand_qty.append((score, c))
        if not cand_qty:
            raise RuntimeError("VENDAS invÃ¡lido: nÃ£o achei Quantidade.")
        cand_qty.sort(reverse=True)
        qcol = cand_qty[0][1]
        df["Quantidade"] = pd.to_numeric(df[qcol].map(br_to_float), errors="coerce").fillna(0).astype(int)
        return df[["SKU", "Quantidade"]].copy()

    raise RuntimeError("Tipo desconhecido.")

# ---------- ExplosÃ£o por KITS ----------
def explodir_por_kits(df: pd.DataFrame, kits: pd.DataFrame, sku_col: str, qtd_col: str) -> pd.DataFrame:
    base = df.copy()
    base["kit_sku"] = base[sku_col].map(norm_sku)
    base["qtd"] = pd.to_numeric(base[qtd_col], errors="coerce").fillna(0).astype(int)
    merged = base.merge(kits, on="kit_sku", how="left")
    exploded = merged.dropna(subset=["component_sku"]).copy()
    exploded["qty"] = pd.to_numeric(exploded["qty"], errors="coerce").fillna(0).astype(int)
    exploded["quantidade_comp"] = exploded["qtd"] * exploded["qty"]
    out = exploded.groupby("component_sku", as_index=False)["quantidade_comp"].sum()
    out = out.rename(columns={"component_sku": "SKU", "quantidade_comp": "Quantidade"})
    return out

# ---------- CÃ¡lculo ----------
def calcular(full_df, fisico_df, vendas_df, cat: "Catalogo", h=60, g=0.0, LT=0):
    kits = cat.kits_reais.copy()
    existentes = set(kits["kit_sku"].unique())
    alias = []
    for s in cat.catalogo_simples["component_sku"].unique().tolist():
        s = norm_sku(s)
        if s and s not in existentes:
            alias.append((s, s, 1))
    if alias:
        kits = pd.concat([kits, pd.DataFrame(alias, columns=["kit_sku", "component_sku", "qty"])], ignore_index=True)
    kits = kits.drop_duplicates(subset=["kit_sku", "component_sku"])

    full = full_df.copy()
    full["SKU"] = full["SKU"].map(norm_sku)
    full["Vendas_Qtd_60d"] = full["Vendas_Qtd_60d"].astype(int)
    full["Estoque_Full"] = full["Estoque_Full"].astype(int)
    full["Em_Transito"] = pd.to_numeric(full["Em_Transito"], errors="coerce").fillna(0).astype(int)

    shp = vendas_df.copy()
    shp["SKU"] = shp["SKU"].map(norm_sku)
    shp["Quantidade_60d"] = shp["Quantidade"].astype(int)

    ml_comp = explodir_por_kits(
        full[["SKU", "Vendas_Qtd_60d"]].rename(columns={"SKU": "kit_sku", "Vendas_Qtd_60d": "Qtd"}),
        kits, "kit_sku", "Qtd"
    ).rename(columns={"Quantidade": "ML_60d"})
    shopee_comp = explodir_por_kits(
        shp[["SKU", "Quantidade_60d"]].rename(columns={"SKU": "kit_sku", "Quantidade_60d": "Qtd"}),
        kits, "kit_sku", "Qtd"
    ).rename(columns={"Quantidade": "Shopee_60d"})

    cat_df = cat.catalogo_simples[["component_sku", "fornecedor", "status_reposicao"]].rename(columns={"component_sku": "SKU"})

    demanda = cat_df.merge(ml_comp, on="SKU", how="left").merge(shopee_comp, on="SKU", how="left")
    demanda[["ML_60d", "Shopee_60d"]] = demanda[["ML_60d", "Shopee_60d"]].apply(pd.to_numeric, errors="coerce").fillna(0).clip(lower=0).astype(int)
    demanda["TOTAL_60d"] = (demanda["ML_60d"] + demanda["Shopee_60d"]).astype(int)

    fis = fisico_df.copy()
    fis["SKU"] = fis["SKU"].map(norm_sku)
    fis["Estoque_Fisico"] = pd.to_numeric(fis["Estoque_Fisico"], errors="coerce").fillna(0).clip(lower=0).astype(int)
    fis["Preco"] = pd.to_numeric(fis["Preco"], errors="coerce").fillna(0.0)

    base = demanda.merge(fis, on="SKU", how="left")
    base["Estoque_Fisico"] = base["Estoque_Fisico"].fillna(0).astype(int)
    base["Preco"] = base["Preco"].fillna(0.0)

    fator = (1.0 + g / 100.0) ** (h / 30.0)
    fk = full.copy()
    fk["vendas_dia"] = fk["Vendas_Qtd_60d"] / 60.0
    fk["alvo"] = np.round(fk["vendas_dia"] * (LT + h) * fator).astype(int)
    fk["oferta"] = (fk["Estoque_Full"] + fk["Em_Transito"]).astype(int)
    fk["envio_desejado"] = (fk["alvo"] - fk["oferta"]).clip(lower=0).astype(int)

    necessidade = explodir_por_kits(
        fk[["SKU", "envio_desejado"]].rename(columns={"SKU": "kit_sku", "envio_desejado": "Qtd"}),
        kits, "kit_sku", "Qtd"
    ).rename(columns={"Quantidade": "Necessidade"})

    base = base.merge(necessidade, on="SKU", how="left")
    base["Necessidade"] = base["Necessidade"].fillna(0).astype(int)

    base["Demanda_dia"] = base["TOTAL_60d"] / 60.0
    base["Reserva_30d"] = np.round(base["Demanda_dia"] * 30).astype(int)
    base["Folga_Fisico"] = (base["Estoque_Fisico"] - base["Reserva_30d"]).clip(lower=0).astype(int)

    base["Compra_Sugerida"] = (base["Necessidade"] - base["Folga_Fisico"]).clip(lower=0).astype(int)

    mask_nao = base["status_reposicao"].str.lower().str.contains("nao_repor", na=False)
    base.loc[mask_nao, "Compra_Sugerida"] = 0
    base = base[~mask_nao]

    base["Valor_Compra_R$"] = (base["Compra_Sugerida"].astype(float) * base["Preco"].astype(float)).round(2)

    base["Vendas_h_ML"] = np.maximum(0, np.round(base["ML_60d"] * (h / 60.0))).astype(int)
    base["Vendas_h_Shopee"] = np.maximum(0, np.round(base["Shopee_60d"] * (h / 60.0))).astype(int)

    base = base.sort_values(["fornecedor", "Valor_Compra_R$", "SKU"], ascending=[True, False, True])

    df_final = base[[
        "SKU", "fornecedor",
        "Vendas_h_ML", "Vendas_h_Shopee",
        "Estoque_Fisico", "Preco", "Compra_Sugerida", "Valor_Compra_R$",
        "ML_60d", "Shopee_60d", "TOTAL_60d", "Reserva_30d", "Folga_Fisico", "Necessidade"
    ]].reset_index(drop=True)

    fis_unid = int(fis["Estoque_Fisico"].sum())
    fis_valor = float((fis["Estoque_Fisico"] * fis["Preco"]).sum())
    full_unid = int(full["Estoque_Full"].sum())
    comp_full = explodir_por_kits(
        full[["SKU", "Estoque_Full"]].rename(columns={"SKU": "kit_sku", "Estoque_Full": "Qtd"}),
        kits, "kit_sku", "Qtd"
    ).merge(fis[["SKU", "Preco"]], on="SKU", how="left")
    full_valor = float((comp_full["Quantidade"].fillna(0) * comp_full["Preco"].fillna(0.0)).sum())

    painel = {"full_unid": full_unid, "full_valor": full_valor, "fisico_unid": fis_unid, "fisico_valor": fis_valor}
    return df_final, painel

# ---------- Export XLSX ----------
def sha256_of_csv(df: pd.DataFrame) -> str:
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    return hashlib.sha256(csv_bytes).hexdigest()

def exportar_xlsx(df_final: pd.DataFrame, h: int, params: dict) -> bytes:
    int_cols = [
        "Vendas_h_ML", "Vendas_h_Shopee", "Estoque_Fisico", "Compra_Sugerida",
        "Reserva_30d", "Folga_Fisico", "Necessidade", "ML_60d", "Shopee_60d", "TOTAL_60d"
    ]
    for c in int_cols:
        if c in df_final.columns:
            df_final[c] = pd.to_numeric(df_final[c], errors="coerce").fillna(0).clip(lower=0).astype(int)
    df_final["Valor_Compra_R$"] = (df_final["Compra_Sugerida"].astype(float) * df_final["Preco"].astype(float)).round(2)

    calc = (df_final["Compra_Sugerida"] * df_final["Preco"]).round(2).values
    if not np.allclose(df_final["Valor_Compra_R$"].values, calc):
        raise RuntimeError("Auditoria: 'Valor_Compra_R$' != 'Compra_Sugerida x Preco'.")

    hash_str = sha256_of_csv(df_final)
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as w:
        lista = df_final[df_final["Compra_Sugerida"] > 0].copy()
        lista.to_excel(w, sheet_name="Lista_Final", index=False)
        ws = w.sheets["Lista_Final"]
        for i, col in enumerate(lista.columns):
            width = max(12, int(lista[col].astype(str).map(len).max()) + 2)
            ws.set_column(i, i, min(width, 40))
        ws.freeze_panes(1, 0)
        ws.autofilter(0, 0, len(lista), len(lista.columns) - 1)

        ctrl = pd.DataFrame([{
            "data_hora": dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "h": h,
            "linhas_Lista_Final": int((df_final["Compra_Sugerida"] > 0).sum()),
            "soma_Compra_Sugerida": int(df_final["Compra_Sugerida"].sum()),
            "soma_Valor_Compra_R$": float(df_final["Valor_Compra_R$"].sum()),
            "hash_sha256": hash_str,
        } | params])
        ctrl.to_excel(w, sheet_name="Controle", index=False)
    output.seek(0)
    return output.read()

# ================== Sidebar ==================
with st.sidebar:
    st.subheader("ParÃ¢metros")
    h = st.selectbox("Horizonte (dias)", [30, 60, 90], index=1)
    g = st.number_input("Crescimento % ao mÃªs", value=0.0, step=1.0)
    LT = st.number_input("Lead time (dias)", value=0, step=1, min_value=0)

with st.sidebar:
    st.markdown("---")
    st.subheader("Estoque FÃ­sico via Tiny v3")
    emp_sel = st.radio("Empresa", ["ALIVVIA", "JCA"], horizontal=True, key="emp_tiny_sel")

    if st.button("ðŸ”„ Sincronizar Estoque (Tiny v3)", use_container_width=True, key="btn_sync_tiny"):
        try:
            skus = _carregar_skus_base(emp_sel)
            if not skus:
                st.error(f"NÃ£o achei SKUs. Carregue o PadrÃ£o (KITS/CAT) e/ou arquivos em .uploads/{emp_sel}/.")
            else:
                df_tiny = sincronizar_estoque_tiny(emp_sel, skus)
                st.session_state["estoque_tiny_por_emp"][emp_sel] = df_tiny
                ok = int((df_tiny["status"] == "OK").sum())
                st.success(f"Tiny v3 ({emp_sel}) sincronizado: {len(df_tiny)} SKUs (OK: {ok}).")
                st.dataframe(df_tiny, use_container_width=True, height=380)
                csv_bytes = df_tiny[["SKU","Estoque_Fisico","Preco"]].to_csv(index=False).encode("utf-8")
                _store_put(emp_sel, "ESTOQUE", "Estoque_Tiny.csv", csv_bytes)
                st.session_state[emp_sel]["ESTOQUE"] = {"name": "Estoque_Tiny.csv", "bytes": csv_bytes}
        except Exception as e:
            st.error(f"Falha ao sincronizar Tiny: {e}")

    st.markdown("---")
    st.subheader("PadrÃ£o (KITS/CAT) â€” Google Sheets")
    colA, colB = st.columns([1, 1])
    with colA:
        if st.button("Carregar padrÃ£o agora", use_container_width=True):
            try:
                content = baixar_xlsx_do_sheets(DEFAULT_SHEET_ID)
                cat = _carregar_padrao_de_content(content)
                st.session_state.catalogo_df = cat.catalogo_simples.rename(columns={"component_sku": "sku"})
                st.session_state.kits_df = cat.kits_reais
                st.session_state.loaded_at = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                st.success("PadrÃ£o carregado.")
            except Exception as e:
                st.session_state.catalogo_df = None
                st.session_state.kits_df = None
                st.error(str(e))
    with colB:
        st.link_button("Abrir no Drive (editar)", DEFAULT_SHEET_LINK, use_container_width=True)

# ================== TÃ­tulo ==================
st.title("ReposiÃ§Ã£o LogÃ­stica â€” Alivvia")
st.caption(f"VersÃ£o: {VERSION}")
st.markdown(f"<div style='text-align:right;color:#999'>VersÃ£o {VERSION}</div>", unsafe_allow_html=True)

if st.session_state.catalogo_df is None or st.session_state.kits_df is None:
    st.warning("â–º Carregue o PadrÃ£o (KITS/CAT) no sidebar antes de usar as abas.")

tab1, tab2, tab3 = st.tabs(["ðŸ“‚ Dados das Empresas", "ðŸ§® Compra AutomÃ¡tica", "ðŸ“¦ AlocaÃ§Ã£o de Compra"])

# ================== TAB 1: Dados ==================
with tab1:
    st.subheader("Uploads fixos por empresa (salvos; permanecem apÃ³s F5)")

    def bloco_empresa(emp: str):
        st.markdown(f"### {emp}")
        c1, c2 = st.columns(2)
        # FULL
        with c1:
            st.markdown(f"**FULL â€” {emp}**")
            up = st.file_uploader("CSV/XLSX/XLS", type=["csv", "xlsx", "xls"], key=f"up_full_{emp}")
            if up is not None:
                blob = up.read()
                st.session_state[emp]["FULL"] = {"name": up.name, "bytes": blob}
                _store_put(emp, "FULL", up.name, blob)
                st.success(f"FULL salvo: {up.name}")
            it = st.session_state[emp]["FULL"]
            if it["name"]:
                st.markdown(badge_ok("FULL salvo", it["name"]), unsafe_allow_html=True)
        # VENDAS
        with c2:
            st.markdown(f"**Shopee/MT â€” {emp}**")
            up = st.file_uploader("CSV/XLSX/XLS", type=["csv", "xlsx", "xls"], key=f"up_vendas_{emp}")
            if up is not None:
                blob = up.read()
                st.session_state[emp]["VENDAS"] = {"name": up.name, "bytes": blob}
                _store_put(emp, "VENDAS", up.name, blob)
                st.success(f"Vendas salvo: {up.name}")
            it = st.session_state[emp]["VENDAS"]
            if it["name"]:
                st.markdown(badge_ok("Vendas salvo", it["name"]), unsafe_allow_html=True)

        # ESTOQUE
        st.markdown("**Estoque FÃ­sico â€” (opcional ou via Tiny)**")
        up = st.file_uploader("CSV/XLSX/XLS", type=["csv", "xlsx", "xls"], key=f"up_est_{emp}")
        if up is not None:
            blob = up.read()
            st.session_state[emp]["ESTOQUE"] = {"name": up.name, "bytes": blob}
            _store_put(emp, "ESTOQUE", up.name, blob)
            st.success(f"Estoque salvo: {up.name}")
        it = st.session_state[emp]["ESTOQUE"]
        if it["name"]:
            st.markdown(badge_ok("Estoque salvo", it["name"]), unsafe_allow_html=True)

    bloco_empresa("ALIVVIA")
    bloco_empresa("JCA")

# ================== TAB 2: Compra AutomÃ¡tica ==================
with tab2:
    st.subheader("Gerar Compra (por empresa) â€” filtros e seleÃ§Ã£o para OC")

    # Helpers da aba
    def _filtro_texto_inteligente(df: pd.DataFrame, texto: str, colunas_busca: list[str]) -> pd.DataFrame:
        if not texto:
            return df
        termos = [t.strip() for t in str(texto).split() if t.strip()]
        if not termos:
            return df
        base = df.copy()
        for col in colunas_busca:
            if col not in base.columns:
                base[col] = ""
            base[col] = base[col].astype(str)
        mask_total = np.ones(len(base), dtype=bool)
        for termo in termos:
            termo_up = termo.upper()
            mask_termo = np.zeros(len(base), dtype=bool)
            for col in colunas_busca:
                mask_termo = mask_termo | base[col].str.upper().str.contains(termo_up, na=False)
            mask_total = mask_total & mask_termo
        return base[mask_total]

    def _preparar_df_compra(base: pd.DataFrame) -> pd.DataFrame:
        df = base.copy()
        if "Descricao" not in df.columns:
            df["Descricao"] = df["SKU"]
        df["Qtd"] = pd.to_numeric(df.get("Compra_Sugerida", 0), errors="coerce").fillna(0).astype(float)
        df["PrecoUnit"] = pd.to_numeric(df.get("Preco", 0.0), errors="coerce").fillna(0.0).astype(float)
        keep = ["SKU", "Descricao", "Qtd", "PrecoUnit"]
        if "fornecedor" in df.columns:
            keep.append("fornecedor")
        df = df[keep].copy()
        df = df[df["Qtd"] > 0]
        df["SKU"] = df["SKU"].astype(str).str.strip().str.upper()
        df["Descricao"] = df["Descricao"].astype(str).str.strip()
        return df.reset_index(drop=True)

    if st.session_state.catalogo_df is None or st.session_state.kits_df is None:
        st.info("Carregue o PadrÃ£o (KITS/CAT) no sidebar.")
    else:
        empresa = st.radio("Empresa ativa", ["ALIVVIA", "JCA"], horizontal=True, key="empresa_ca")
        dados = st.session_state[empresa]

        col = st.columns(3)
        col[0].info(f"FULL: {dados['FULL']['name'] or 'â€”'}")
        col[1].info(f"Shopee/MT: {dados['VENDAS']['name'] or 'â€”'}")
        col[2].info(f"Estoque: {dados['ESTOQUE']['name'] or 'â€”'}")

        if st.button(f"Gerar Compra â€” {empresa}", type="primary", key=f"btn_calc_{empresa}"):
            try:
                for k, rot in [("FULL", "FULL"), ("VENDAS", "Shopee/MT"), ("ESTOQUE", "Estoque")]:
                    if not (dados[k]["name"] and dados[k]["bytes"]):
                        raise RuntimeError(f"Arquivo '{rot}' nÃ£o foi salvo para {empresa}. Use a aba Dados das Empresas.")

                full_raw = load_any_table_from_bytes(dados["FULL"]["name"], dados["FULL"]["bytes"])
                vendas_raw = load_any_table_from_bytes(dados["VENDAS"]["name"], dados["VENDAS"]["bytes"])
                fisico_raw = load_any_table_from_bytes(dados["ESTOQUE"]["name"], dados["ESTOQUE"]["bytes"])

                t_full = mapear_tipo(full_raw)
                t_v = mapear_tipo(vendas_raw)
                t_f = mapear_tipo(fisico_raw)
                if t_full != "FULL":   raise RuntimeError("FULL invÃ¡lido.")
                if t_v != "VENDAS":    raise RuntimeError("Vendas invÃ¡lido.")
                if t_f != "FISICO":    raise RuntimeError("Estoque invÃ¡lido.")

                full_df = mapear_colunas(full_raw, t_full)
                vendas_df = mapear_colunas(vendas_raw, t_v)
                fisico_df = mapear_colunas(fisico_raw, t_f)

                cat = Catalogo(
                    catalogo_simples=st.session_state.catalogo_df.rename(columns={"sku": "component_sku"}),
                    kits_reais=st.session_state.kits_df
                )
                df_final, painel = calcular(full_df, fisico_df, vendas_df, cat, h=h, g=g, LT=LT)

                st.session_state["resultado_compra"][empresa] = {"df": df_final, "painel": painel}
                st.session_state["oc_selection"][empresa] = set()  # zera seleÃ§Ã£o desta empresa
                st.success("CÃ¡lculo concluÃ­do e salvo. Aplique filtros e selecione os itens.")
            except Exception as e:
                st.error(str(e))

        # ExibiÃ§Ã£o do resultado desta EMPRESA
        if empresa in st.session_state.get("resultado_compra", {}):
            pkg = st.session_state["resultado_compra"][empresa]
            df_final = pkg["df"].copy()
            painel = pkg["painel"]

            # MÃ©tricas gerais
            cA, cB, cC, cD = st.columns(4)
            cA.metric("Full (un)", f"{painel['full_unid']:,}".replace(",", "."))
            cB.metric("Full (R$)", f"R$ {painel['full_valor']:,.2f}")
            cC.metric("FÃ­sico (un)", f"{painel['fisico_unid']:,}".replace(",", "."))
            cD.metric("FÃ­sico (R$)", f"R$ {painel['fisico_valor']:,.2f}")

       # â€”â€”â€” ORION/ATHENAS: filtros inteligentes + seleÃ§Ã£o persistente â€”â€”â€”
df_view, sel_set = bloco_filtros_e_selecao(df_final, empresa, state_key_prefix="oc")

# â€”â€” BotÃµes de envio para a Ordem de Compra â€”â€”
col_send1, col_send2, col_send3 = st.columns([1,1,1])

with col_send1:
    if st.button("âž¡ï¸ Enviar **SELECIONADOS** para a Ordem de Compra", use_container_width=True, key=f"oc_send_sel_{empresa}"):
        if not sel_set:
            st.warning("Nenhum SKU selecionado. Marque ou use os botÃµes de seleÃ§Ã£o rÃ¡pida.")
        else:
            base_sel = df_final[df_final["SKU"].astype(str).isin(list(sel_set))]
            df_export = preparar_df_para_oc(base_sel)
            if df_export.empty:
                st.warning("Os selecionados nÃ£o tÃªm Compra_Sugerida > 0.")
            else:
                st.session_state["df_compra"] = df_export
                st.success(f"{len(df_export)} itens selecionados enviados para a pÃ¡gina ðŸ§¾ Ordem de Compra.")

with col_send2:
    if st.button("âž¡ï¸ Enviar ITENS **FILTRADOS** para a Ordem de Compra", use_container_width=True, key=f"oc_send_filtrado_{empresa}"):
        df_export = preparar_df_para_oc(df_view)
        if df_export.empty:
            st.warning("Nada para enviar: ajuste os filtros ou gere a compra novamente.")
        else:
            st.session_state["df_compra"] = df_export
            st.success(f"{len(df_export)} itens (filtrados) enviados para a pÃ¡gina ðŸ§¾ Ordem de Compra.")

with col_send3:
    if st.button("âž¡ï¸ Enviar **TODA** a compra (sem filtro) para a Ordem de Compra", use_container_width=True, key=f"oc_send_tudo_{empresa}"):
        df_export = preparar_df_para_oc(df_final)
        if df_export.empty:
            st.warning("Nada para enviar: gere a compra novamente.")
        else:
            st.session_state["df_compra"] = df_export
            st.success(f"{len(df_export)} itens (todos) enviados para a pÃ¡gina ðŸ§¾ Ordem de Compra.")

            with col_send1:
                if st.button("âž¡ï¸ Enviar **SELECIONADOS** para a Ordem de Compra", use_container_width=True, key=f"oc_send_sel_{empresa}"):
                    try:
                        if not sel_set:
                            st.warning("Nenhum SKU selecionado. Marque ou use os botÃµes de seleÃ§Ã£o rÃ¡pida.")
                        else:
                            base_sel = st.session_state["resultado_compra"][empresa]["df"]
                            base_sel = base_sel[base_sel["SKU"].astype(str).isin(list(sel_set))]
                            df_export = _preparar_df_compra(base_sel)
                            if df_export.empty:
                                st.warning("Os selecionados nÃ£o tÃªm Compra_Sugerida > 0.")
                            else:
                                st.session_state["df_compra"] = df_export
                                st.success(f"{len(df_export)} itens selecionados enviados para a pÃ¡gina ðŸ§¾ Ordem de Compra.")
                    except Exception as e:
                        st.error(f"Falha no envio dos selecionados: {e}")
            with col_send2:
                if st.button("âž¡ï¸ Enviar ITENS **FILTRADOS** para a Ordem de Compra", use_container_width=True, key=f"oc_send_filtrado_{empresa}"):
                    try:
                        df_export = _preparar_df_compra(df_view)
                        if df_export.empty:
                            st.warning("Nada para enviar: ajuste os filtros ou gere a compra novamente.")
                        else:
                            st.session_state["df_compra"] = df_export
                            st.success(f"{len(df_export)} itens (filtrados) enviados para a pÃ¡gina ðŸ§¾ Ordem de Compra.")
                    except Exception as e:
                        st.error(f"Falha no envio filtrado: {e}")
            with col_send3:
                if st.button("âž¡ï¸ Enviar **TODA** a compra (sem filtro) para a Ordem de Compra", use_container_width=True, key=f"oc_send_tudo_{empresa}"):
                    try:
                        df_export = _preparar_df_compra(st.session_state["resultado_compra"][empresa]["df"])
                        if df_export.empty:
                            st.warning("Nada para enviar: gere a compra novamente.")
                        else:
                            st.session_state["df_compra"] = df_export
                            st.success(f"{len(df_export)} itens (todos) enviados para a pÃ¡gina ðŸ§¾ Ordem de Compra.")
                    except Exception as e:
                        st.error(f"Falha no envio total: {e}")

            # Downloads (opcional)
            colx1, colx2 = st.columns([1, 1])
            with colx1:
                try:
                    xlsx_all = exportar_xlsx(st.session_state["resultado_compra"][empresa]["df"], h=h, params={"g": g, "LT": LT, "empresa": empresa})
                    st.download_button(
                        "Baixar XLSX (completo)", data=xlsx_all,
                        file_name=f"Compra_Sugerida_{empresa}_{h}d.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key=f"d_all_{empresa}"
                    )
                except Exception as e:
                    st.error(f"Falha ao gerar XLSX completo: {e}")
            with colx2:
                try:
                    xlsx_filtrado = exportar_xlsx(df_view, h=h, params={"g": g, "LT": LT, "empresa": empresa, "filtro": "on"})
                    st.download_button(
                        "Baixar XLSX (filtrado)", data=xlsx_filtrado,
                        file_name=f"Compra_Sugerida_{empresa}_{h}d_filtrado.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key=f"d_fil_{empresa}"
                    )
                except Exception as e:
                    st.error(f"Falha ao gerar XLSX filtrado: {e}")

            # Comparador ALIVVIA x JCA por SKU (opcional)
            with st.expander("ðŸ”Ž Buscar SKU e comparar compra sugerida nas duas contas", expanded=False):
                sku_query = st.text_input(
                    "SKU exato do componente (nÃ£o kit)",
                    key=f"cmp_sku_query_{empresa}",
                    placeholder="Ex.: LUVA-NEOPRENE-PRETA-G"
                )
                st.caption("Dica: gere a compra nas duas empresas para a comparaÃ§Ã£o funcionar.")
                if st.button("Comparar ALIVVIA x JCA", key=f"btn_cmp_duas_contas_{empresa}"):
                    try:
                        alvo = norm_sku(sku_query)
                        if not alvo:
                            st.warning("Informe um SKU.")
                        else:
                            rows = []
                            for emp_cmp in ["ALIVVIA", "JCA"]:
                                res_emp = st.session_state.get("resultado_compra", {}).get(emp_cmp)
                                if not res_emp:
                                    rows.append({"Empresa": emp_cmp, "SKU": alvo, "Compra_Sugerida": None, "Preco": None, "Valor_Compra_R$": None, "Obs": "Gere a compra para esta empresa"})
                                    continue
                                df_emp = res_emp["df"]
                                r = df_emp[df_emp["SKU"] == alvo]
                                if r.empty:
                                    rows.append({"Empresa": emp_cmp, "SKU": alvo, "Compra_Sugerida": 0, "Preco": None, "Valor_Compra_R$": None, "Obs": "SKU nÃ£o encontrado no resultado"})
                                else:
                                    r0 = r.iloc[0]
                                    rows.append({
                                        "Empresa": emp_cmp,
                                        "SKU": r0["SKU"],
                                        "Compra_Sugerida": int(r0.get("Compra_Sugerida", 0)),
                                        "Preco": float(r0.get("Preco", 0.0)),
                                        "Valor_Compra_R$": float(r0.get("Valor_Compra_R$", 0.0)),
                                        "Obs": ""
                                    })
                            cmp_df = pd.DataFrame(rows)
                            st.dataframe(cmp_df, use_container_width=True, hide_index=True)
                            st.download_button(
                                "Baixar comparaÃ§Ã£o (.csv)",
                                data=cmp_df.to_csv(index=False).encode("utf-8"),
                                file_name=f"Comparacao_ALIVVIA_JCA_{alvo}.csv",
                                mime="text/csv",
                                key=f"dl_cmp_duas_contas_{empresa}"
                            )
                    except Exception as e:
                        st.error(f"Falha na comparaÃ§Ã£o: {e}")
        else:
            st.info("Clique Gerar Compra para calcular e entÃ£o aplicar filtros.")

# ================== TAB 3: AlocaÃ§Ã£o de Compra ==================
with tab3:
    st.subheader("Distribuir quantidade entre empresas â€” proporcional Ã s vendas (FULL + Shopee)")
    if st.session_state.catalogo_df is None or st.session_state.kits_df is None:
        st.info("Carregue o PadrÃ£o (KITS/CAT) no sidebar.")
    else:
        CATALOGO = st.session_state.catalogo_df
        sku_opcoes = CATALOGO["sku"].dropna().astype(str).sort_values().unique().tolist()
        sku_escolhido = st.selectbox("SKU do componente para alocar", ["(selecione)"] + sku_opcoes, key="alloc_sku")
        qtd_lote = st.number_input("Quantidade total do lote", min_value=1, value=1000, step=50)

        st.caption("Necessita FULL e Shopee/MT salvos para ALIVVIA e JCA na aba Dados.")

        if st.button("Calcular alocaÃ§Ã£o proporcional", type="primary"):
            try:
                if sku_escolhido in ("", "(selecione)"):
                    raise RuntimeError("Escolha um SKU para alocar.")

                missing = []
                for emp in ["ALIVVIA", "JCA"]:
                    if not (st.session_state[emp]["FULL"]["name"] and st.session_state[emp]["FULL"]["bytes"]):
                        missing.append(f"{emp} FULL")
                    if not (st.session_state[emp]["VENDAS"]["name"] and st.session_state[emp]["VENDAS"]["bytes"]):
                        missing.append(f"{emp} Shopee/MT")
                if missing:
                    raise RuntimeError("Faltam arquivos salvos: " + ", ".join(missing))

                def read_pair(emp: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
                    fa = load_any_table_from_bytes(st.session_state[emp]["FULL"]["name"], st.session_state[emp]["FULL"]["bytes"])
                    sa = load_any_table_from_bytes(st.session_state[emp]["VENDAS"]["name"], st.session_state[emp]["VENDAS"]["bytes"])
                    tfa = mapear_tipo(fa); tsa = mapear_tipo(sa)
                    if tfa != "FULL":
                        raise RuntimeError(f"FULL invÃ¡lido ({emp}).")
                    if tsa != "VENDAS":
                        raise RuntimeError(f"Vendas invÃ¡lido ({emp}).")
                    return mapear_colunas(fa, tfa), mapear_colunas(sa, tsa)

                full_A, shp_A = read_pair("ALIVVIA")
                full_J, shp_J = read_pair("JCA")

                cat = Catalogo(
                    catalogo_simples=CATALOGO.rename(columns={"sku": "component_sku"}),
                    kits_reais=st.session_state.kits_df
                )

                def vendas_componente(full_df, shp_df) -> pd.DataFrame:
                    kits = cat.kits_reais
                    a = explodir_por_kits(
                        full_df[["SKU", "Vendas_Qtd_60d"]].rename(columns={"SKU": "kit_sku", "Vendas_Qtd_60d": "Qtd"}),
                        kits, "kit_sku", "Qtd"
                    ).rename(columns={"Quantidade": "ML_60d"})
                    b = explodir_por_kits(
                        shp_df[["SKU", "Quantidade"]].rename(columns={"SKU": "kit_sku", "Quantidade": "Qtd"}),
                        kits, "kit_sku", "Qtd"
                    ).rename(columns={"Quantidade": "Shopee_60d"})
                    out = pd.merge(a, b, on="SKU", how="outer").fillna(0)
                    out["Demanda_60d"] = out["ML_60d"].astype(int) + out["Shopee_60d"].astype(int)
                    return out[["SKU", "Demanda_60d"]]

                demA = vendas_componente(full_A, shp_A)
                demJ = vendas_componente(full_J, shp_J)

                sku_norm = norm_sku(sku_escolhido)
                dA = int(demA.loc[demA["SKU"] == sku_norm, "Demanda_60d"].sum())
                dJ = int(demJ.loc[demJ["SKU"] == sku_norm, "Demanda_60d"].sum())
                total = dA + dJ

                if total == 0:
                    st.warning("Sem vendas detectadas; alocaÃ§Ã£o 50/50.")
                    propA = propJ = 0.5
                else:
                    propA = dA / total
                    propJ = dJ / total

                alocA = int(round(qtd_lote * propA))
                alocJ = int(qtd_lote - alocA)

                res = pd.DataFrame([
                    {"Empresa": "ALIVVIA", "SKU": sku_norm, "Demanda_60d": dA, "Proporcao": round(propA, 4), "Alocacao_Sugerida": alocA},
                    {"Empresa": "JCA", "SKU": sku_norm, "Demanda_60d": dJ, "Proporcao": round(propJ, 4), "Alocacao_Sugerida": alocJ},
                ])
                st.dataframe(res, use_container_width=True)
                st.success(f"Total alocado: {qtd_lote} un (ALIVVIA {alocA} | JCA {alocJ})")
                st.download_button(
                    "Baixar alocaÃ§Ã£o (.csv)",
                    data=res.to_csv(index=False).encode("utf-8"),
                    file_name=f"Alocacao_{sku_norm}_{qtd_lote}.csv",
                    mime="text/csv"
                )
            except Exception as e:
                st.error(f"Falha na alocaÃ§Ã£o: {e}")

# ---------- RodapÃ© ----------
st.caption(f"Â© Alivvia â€” {VERSION}")

