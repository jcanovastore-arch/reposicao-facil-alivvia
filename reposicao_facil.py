# ====== INÍCIO: HELPERS v3 PARA ESTOQUE ======
import os, json, time
import requests
import pandas as pd
import streamlit as st

_TINY_V3_BASE = "https://erp.tiny.com.br/public-api/v3"

def _tiny_v3_token_path(emp: str) -> str:
    """Arquivo de token por empresa (ex.: tokens/tiny_JCA.json, tokens/tiny_ALIVVIA.json)."""
    os.makedirs("tokens", exist_ok=True)
    return os.path.join("tokens", f"tiny_{emp}.json")

def _tiny_v3_load_access_token(emp: str) -> str:
    """Carrega o access_token do arquivo JSON da empresa."""
    p = _tiny_v3_token_path(emp)
    if not os.path.exists(p):
        raise RuntimeError(f"Token v3 da empresa {emp} não encontrado em {p}.")
    with open(p, "r", encoding="utf-8") as f:
        data = json.load(f)
    tok = data.get("access_token")
    if not tok:
        raise RuntimeError(f"Arquivo {p} não contém 'access_token'.")
    return tok

def _tiny_v3_req(token: str, method: str, path: str, params=None):
    """Chamada HTTP com tratamento de erros e re tentativas."""
    url = f"{_TINY_V3_BASE}/{path.lstrip('/')}"
    headers = {"Authorization": f"Bearer {token}"}
    for attempt in range(3):
        r = requests.request(method, url, params=params, headers=headers, timeout=30)
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(1.5 * (attempt + 1))
            continue
        if r.status_code == 401:
            raise RuntimeError("401 Unauthorized: token inválido ou expirado.")
        if not r.ok:
            raise RuntimeError(f"HTTP {r.status_code} em {path}: {r.text[:200]}")
        try:
            return r.json()
        except Exception:
            raise RuntimeError(f"Resposta não-JSON em {path}: {r.text[:200]}")
    raise RuntimeError(f"Falha repetida em {path}")

def _tiny_v3_get_id_por_sku(token: str, sku: str) -> int | None:
    """Busca ID do produto pelo SKU em /produtos?codigo=."""
    data = _tiny_v3_req(token, "GET", "/produtos", params={"codigo": sku})
    itens = data.get("itens") or data.get("items") or data.get("data") or []
    if not itens:
        return None
    pid = itens[0].get("id")
    return int(pid) if pid else None

def _tiny_v3_get_estoque_geral(token: str, product_id: int) -> dict | None:
    """Retorna saldo/reservado/disponível do depósito 'Geral'."""
    data = _tiny_v3_req(token, "GET", f"/estoque/{product_id}")
    depositos = data.get("depositos") or data.get("data", {}).get("depositos") or []
    if not depositos:
        return None
    for d in depositos:
        nome = (d.get("nome") or "").strip().lower()
        if nome == "geral" or d.get("desconsiderar") is False:
            return {
                "deposito_id": d.get("id"),
                "deposito_nome": d.get("nome"),
                "saldo": d.get("saldo"),
                "reservado": d.get("reservado"),
                "disponivel": d.get("disponivel"),
            }
    return None

def _carregar_skus_base(emp: str) -> list[str]:
    """
    Busca SKUs automaticamente:
    1) SessionState df_padrao_<EMP>
    2) Último arquivo PADRAO/KITS/CAT em .uploads/<EMP>/
    """
    key = f"df_padrao_{emp}"
    if key in st.session_state:
        df = st.session_state[key]
        for col in ("SKU", "codigo", "sku"):
            if col in df.columns:
                skus = df[col].dropna().astype(str).str.strip().unique().tolist()
                if skus:
                    return skus

    base = os.path.join(".uploads", emp.upper())
    if os.path.isdir(base):
        for root, _, files in os.walk(base):
            cand = [f for f in files if any(k in f.upper() for k in ("PADRAO", "KITS", "CAT")) and f.lower().endswith((".csv", ".xlsx"))]
            cand.sort(reverse=True)
            if cand:
                path = os.path.join(root, cand[0])
                try:
                    df = pd.read_excel(path) if path.lower().endswith(".xlsx") else pd.read_csv(path, sep=None, engine="python")
                    for col in ("SKU", "codigo", "sku"):
                        if col in df.columns:
                            skus = df[col].dropna().astype(str).str.strip().unique().tolist()
                            if skus:
                                return skus
                except Exception:
                    pass
    return []

def _sincronizar_estoque_v3(emp: str) -> pd.DataFrame:
    """Sincroniza o estoque do Tiny v3 (depósito Geral) para uma empresa."""
    token = _tiny_v3_load_access_token(emp)
    skus = _carregar_skus_base(emp)
    if not skus:
        raise RuntimeError(f"Não encontrei SKUs para {emp}. Abra o PADRÃO/KITS/CAT primeiro.")
    linhas = []
    for sku in skus:
        try:
            pid = _tiny_v3_get_id_por_sku(token, sku)
            if not pid:
                linhas.append({"SKU": sku, "status": "SKU não encontrado"})
                continue
            est = _tiny_v3_get_estoque_geral(token, pid)
            if not est:
                linhas.append({"SKU": sku, "product_id": pid, "status": "Sem depósito 'Geral'"})
                continue
            linhas.append({
                "SKU": sku,
                "product_id": pid,
                "deposito_nome": est["deposito_nome"],
                "saldo": est["saldo"],
                "reservado": est["reservado"],
                "disponivel": est["disponivel"],
                "status": "OK",
            })
        except Exception as e:
            linhas.append({"SKU": sku, "status": f"ERRO: {e}"})
    return pd.DataFrame(linhas)

def _salvar_estoque_upload(emp: str, df: pd.DataFrame) -> str:
    """Salva o DF como CSV em .uploads/<EMP>/ESTOQUE/ESTOQUE_ATUAL.csv."""
    out_dir = os.path.join(".uploads", emp.upper(), "ESTOQUE")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "ESTOQUE_ATUAL.csv")
    df.to_csv(out_path, index=False, encoding="utf-8")
    return out_path
# ====== FIM: HELPERS v3 PARA ESTOQUE ======


def render_estoque_section(emp: str):
    """
    Substitui a função antiga.
    - Ao abrir a aba, sincroniza estoque com Tiny v3 (usando tokens/tiny_<EMP>.json)
    - Mostra tabela do depósito 'Geral'
    - Botão “🔄 Forçar sincronização com o Tiny” repete o processo
    """
    st.subheader(f"📦 Estoque — {emp}")

    def _sync_and_show():
        try:
            df = _sincronizar_estoque_v3(emp)
            path = _salvar_estoque_upload(emp, df)
            st.success(f"Estoque atualizado via Tiny v3 ({emp}). Salvo em: {path}")
            st.dataframe(df, use_container_width=True, hide_index=True)
            st.session_state[f"df_estoque_{emp}"] = df
        except Exception as e:
            st.error(f"Falha ao sincronizar estoque ({emp}): {e}")

    # Auto-sincroniza ao abrir
    _sync_and_show()

    # Botão para forçar sincronização
    if st.button("🔄 Forçar sincronização com o Tiny", key=f"btn_sync_{emp}"):
        _sync_and_show()
