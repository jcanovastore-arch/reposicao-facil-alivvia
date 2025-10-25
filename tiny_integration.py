# tiny_integration.py
import os, io, time, unicodedata, requests, pandas as pd, streamlit as st

# ------------------ CONFIG TINY ------------------
_TINY_LIST_URL  = "https://api.tiny.com.br/api2/produtos.pesquisa.php"
_TINY_STOCK_URL = "https://api.tiny.com.br/api2/produto.obter.estoque.php"

# Tokens (pode mover para secrets.toml depois)
_TINY_TOKENS = {
    "ALIVVIA": "b3ca9c3319ac75276c03e097296e15619259cab9029a1e45b781a07553bdb25b",
    "JCA":     "352880e9498ec1a29b81a9f0ea1a946a46415f93b2aa2706634f39064b750dcd",
}

# Depósito alvo e dicas (multiempresas)
_TINY_DEP_ALVO = "geral"  # contém "geral" (sem acento/caixa)
_TINY_HINTS = {
    "ALIVVIA": ["piva", "alivvia", "pivastore"],
    "JCA":     ["jca", "jcanova", "jcanovastore"],
}
_TINY_GERAL_IDX = {"ALIVVIA": 0, "JCA": 0}  # fallback: 0=1º "Geral", 1=2º...

# ------------------ UTILS ------------------
def _norm_str(s: str) -> str:
    s = str(s or "")
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return s

def _norm(s: str) -> str:
    return _norm_str(s).lower().strip()

def _to_float(x) -> float:
    if x in (None, ""): return 0.0
    if isinstance(x, (int, float)): return float(x)
    s = _norm_str(x).strip().replace(".", "").replace(",", ".")
    try: return float(s)
    except: return 0.0

def _norm_code(s: str) -> str:
    return _norm_str(s).strip().upper()

# ------------------ PLANILHA PADRÃO ------------------
def _load_skus_padrao(conta: str) -> set:
    """
    Procura a planilha padrão em .uploads/<CONTA>/produto_padrao.xlsx (ou .csv)
    e devolve um set com os SKUs normalizados (coluna 'SKU').
    """
    base_dir = os.path.join(os.getcwd(), ".uploads", conta)
    candidatos = [
        os.path.join(base_dir, "produto_padrao.xlsx"),
        os.path.join(base_dir, "produto_padrao.csv"),
        os.path.join(base_dir, "produtos_padrao.xlsx"),
        os.path.join(base_dir, "produtos_padrao.csv"),
    ]
    for path in candidatos:
        if os.path.exists(path):
            try:
                if path.lower().endswith(".csv"):
                    df = pd.read_csv(path, dtype=str)
                else:
                    df = pd.read_excel(path, dtype=str)
                if "SKU" in df.columns:
                    return { _norm_code(x) for x in df["SKU"].dropna().tolist() }
            except Exception:
                pass
    return set()

# ------------------ TINY API ------------------
def _listar_basicos(token: str):
    """Lista: id, codigo, situacao, tipoVariacao, preco_custo_medio."""
    items, page = [], 1
    sess = requests.Session()
    sess.headers.update({"User-Agent": "ReposicaoApp/Tiny"})
    while True:
        r = sess.post(_TINY_LIST_URL, data={"token": token, "formato": "json", "pagina": page}, timeout=40)
        if r.status_code != 200: break
        data = r.json() if r.text else {}
        bloco = (data.get("retorno", {}) or {}).get("produtos") or []
        if not bloco: break
        for it in bloco:
            items.append(it.get("produto", {}) or {})
        page += 1
    return items

def _obter_estoque(token: str, prod_id):
    """Endpoint oficial de estoque — retorna depósitos + (às vezes) preço."""
    payload = {
        "token": token, "formato": "json", "id": prod_id,
        "incluirDepositos":"S", "retornarDepositos":"S", "detalharDepositos":"S",
    }
    r = requests.post(_TINY_STOCK_URL, data=payload, timeout=40)
    if r.status_code != 200: return {}
    ret = (r.json() if r.text else {}).get("retorno", {}) or {}
    return ret.get("produto") or ret.get("estoque") or {}

def _estoque_geral_da_conta(conta: str, det: dict) -> float:
    """Escolhe o depósito 'Geral' da CONTA (por nome/empresa; senão, por índice)."""
    depositos = det.get("depositos") or det.get("estoques") or []
    if not depositos: return 0.0
    gerais = []
    for dep in depositos:
        d = dep.get("deposito", dep) or {}
        if str(d.get("desconsiderar","")).upper() == "S":
            continue
        nome_dep = _norm(d.get("nome") or d.get("descricao"))
        if _TINY_DEP_ALVO in nome_dep:
            gerais.append(d)
    if not gerais: return 0.0

    # Preferência por empresa/filial
    hints = _TINY_HINTS.get(conta, [])
    for d in gerais:
        texto_emp = " ".join(_norm(d.get(k)) for k in (
            "empresa","filial","empresa_nome","nomeEmpresa","empresaDescricao","empresaDescricaoCurta","cnpjEmpresa"
        ) if d.get(k) is not None)
        if any(h in texto_emp for h in hints):
            return _to_float(d.get("saldo"))

    # Fallback por ordem
    idx = max(0, int(_TINY_GERAL_IDX.get(conta, 0)))
    if idx < len(gerais):
        return _to_float(gerais[idx].get("saldo"))
    return 0.0

def _montar_df(conta: str) -> pd.DataFrame:
    """
    Gera DF com colunas: SKU, Estoque_Fisico, Preco (somente depósito 'Geral').
    Concilia com planilha padrão (.uploads/<conta>/produto_padrao.*) — mantém apenas SKUs presentes.
    """
    token = _TINY_TOKENS[conta]
    base = _listar_basicos(token)
    skus_padrao = _load_skus_padrao(conta)

    linhas = []
    for pb in base:
        # filtra pais/kit e inativos
        if pb.get("situacao") != "A":
            continue
        if pb.get("tipoVariacao") not in {"N","V"}:
            continue

        det = _obter_estoque(token, pb.get("id")) or {}
        sku_raw = (pb.get("codigo") or det.get("codigo") or "").strip()
        if not sku_raw:
            continue

        # aplica filtro por planilha padrão (se existir)
        if skus_padrao and _norm_code(sku_raw) not in skus_padrao:
            continue

        estoque = _estoque_geral_da_conta(conta, det)
        preco   = _to_float(det.get("preco_custo_medio")) or _to_float(pb.get("preco_custo_medio"))
        linhas.append({"SKU": sku_raw, "Estoque_Fisico": estoque, "Preco": preco})

    df = pd.DataFrame(linhas, columns=["SKU","Estoque_Fisico","Preco"]).sort_values("SKU").reset_index(drop=True)

    if not skus_padrao:
        st.warning(
            f"Não encontrei a planilha padrão de produtos de {conta} "
            f"(.uploads/{conta}/produto_padrao.xlsx ou .csv). "
            "O estoque Tiny foi carregado **sem filtro**.",
            icon="⚠️"
        )
    return df

# ------------------ RENDER UI (drop-in) ------------------
def render_estoque_section(emp: str, store_put, store_delete, badge_ok):
    """
    Substitui o bloco do upload de 'Estoque Físico — opcional' no app.
    - Auto-sincroniza com Tiny (depósito 'Geral', multiempresas tratado, filtro por planilha padrão).
    - Botão grande para forçar sincronização.
    - Salva em sessão + cofre usando as mesmas funções do app (store_put/store_delete).
    """
    st.markdown("**Estoque Físico — origem: Tiny (depósito ‘Geral’ + planilha padrão)**")

    def _fetch_and_store(conta: str):
        df = _montar_df(conta)
        buf = io.BytesIO()
        df.to_excel(buf, index=False)
        blob = buf.getvalue()
        name = f"estoque_api_{conta}_{int(time.time())}.xlsx"
        st.session_state[conta]["ESTOQUE"] = {"name": name, "bytes": blob}
        store_put(conta, "ESTOQUE", name, blob)
        return df, name

    # Auto-sync na primeira carga (se ainda não houver estoque em sessão)
    if not st.session_state[emp]["ESTOQUE"]["name"]:
        with st.spinner(f"Sincronizando {emp} com o Tiny…"):
            df_sync, fname = _fetch_and_store(emp)
        st.success(f"Estoque atualizado via API: {fname}")

    # Botão para forçar sincronização
    if st.button("🔄 Forçar sincronização com o Tiny", key=f"force_sync_{emp}", use_container_width=True):
        with st.spinner(f"Atualizando {emp} (Tiny)…"):
            df_sync, fname = _fetch_and_store(emp)
        st.success(f"Estoque atualizado via API: {fname}")

    # Badge e opção de limpar — mantém o fluxo do app
    it = st.session_state[emp]["ESTOQUE"]
    if it["name"]:
        st.markdown(badge_ok("Estoque salvo", it["name"]), unsafe_allow_html=True)
        if st.button("Limpar Estoque (somente este)", key=f"clr_{emp}_ESTOQUE", use_container_width=True):
            store_delete(emp, "ESTOQUE")
            st.info("Estoque removido.")
