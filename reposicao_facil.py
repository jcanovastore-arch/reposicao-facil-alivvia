# -*- coding: utf-8 -*-
import io
import re
import requests
import numpy as np
import pandas as pd
from unidecode import unidecode
import streamlit as st

# =========================================================
# CONFIGURAÇÕES
# =========================================================
st.set_page_config(page_title="Reposição Logística — Alivvia", layout="wide")

# >>>>>>>>>>>> PLANILHA FIXA (SUA) — NÃO PRECISA PREENCHER NADA <<<<<<<<<<<<<
SHEET_ID_DEFAULT = "1cTLARjq-B5g50dL6tcntg7lb_Iu0ta43"  # <-- sua planilha fixa

# Nomes de abas esperadas no Google Sheets
TAB_KITS = "KITS"
TAB_CAT = "CATALOGO_SIMPLES"

# =========================================================
# FUNÇÕES UTILITÁRIAS
# =========================================================
def _norm(s: str) -> str:
    if s is None:
        return ""
    s = unidecode(str(s)).strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s

def _read_google_sheet_xlsx(sheet_id: str) -> bytes:
    """Baixa toda a planilha Google Sheets como XLSX."""
    url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=xlsx"
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return r.content

def _read_sheet_tab(xlsx_bytes: bytes, sheet_name: str) -> pd.DataFrame:
    """Lê uma aba específica como DataFrame."""
    with io.BytesIO(xlsx_bytes) as fh:
        try:
            df = pd.read_excel(fh, sheet_name=sheet_name, engine="openpyxl")
        except Exception:
            df = pd.read_excel(fh, sheet_name=sheet_name)
    return df

def _try_map_columns(df: pd.DataFrame, mapping: dict, required: list, ctx: str):
    """
    Faz mapeamento tolerante de colunas para padronizar nomes.
    mapping = {destino: [possiveis_nomes]}
    required = ['sku', ...]
    ctx = contexto para mensagem de erro.
    """
    cols = {_norm(c): c for c in df.columns}
    new_cols = {}
    for dest, options in mapping.items():
        achou = None
        for opt in options:
            optn = _norm(opt)
            # match exato
            if optn in cols:
                achou = cols[optn]
                break
            # match por "contém"
            for k_norm, k_raw in cols.items():
                if optn in k_norm:
                    achou = k_raw
                    break
            if achou:
                break
        if achou:
            new_cols[achou] = dest

    df2 = df.rename(columns=new_cols).copy()
    faltantes = [c for c in required if c not in df2.columns]
    if faltantes:
        raise KeyError(
            f"Colunas obrigatórias ausentes em {ctx}: {faltantes}\n"
            f"Colunas lidas: {list(df.columns)}"
        )
    return df2

# ---------------------------------------------------------
# LEITURA DO GOOGLE SHEETS (sempre)
# ---------------------------------------------------------
@st.cache_data(show_spinner=False, ttl=5*60)
def carregar_kits_catalogo(sheet_id: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Sempre baixa do Google Sheets (xlsx) e devolve (kits, catalogo).
    """
    xlsx = _read_google_sheet_xlsx(sheet_id)
    kits = _read_sheet_tab(xlsx, TAB_KITS)
    catalogo = _read_sheet_tab(xlsx, TAB_CAT)

    kits_map = {
        "kit_sku": ["kit_sku", "sku_kit", "sku kit"],
        "component_sku": ["component_sku", "sku_componente", "componente", "component", "sku componente"],
        "qty_por_kit": ["qty_por_kit", "quantidade", "qtd_por_kit", "qtd", "quantidade_por_kit"]
    }
    kits = _try_map_columns(kits, kits_map, ["kit_sku", "component_sku", "qty_por_kit"], "KITS")

    cat_map = {
        "sku": ["sku", "codigo", "codigo_sku", "id_sku", "produto"],
        "fornecedor": ["fornecedor", "vendor", "fabricante", "marca"],
        "preco": ["preco", "valor", "custo", "preco_unitario"]
    }
    catalogo = _try_map_columns(catalogo, cat_map, ["sku", "fornecedor", "preco"], "CATALOGO")

    # Normalizações
    for col in ["kit_sku", "component_sku", "sku", "fornecedor"]:
        if col in kits.columns:
            kits[col] = kits[col].astype(str).str.strip()
        if col in catalogo.columns:
            catalogo[col] = catalogo[col].astype(str).str.strip()

    if "qty_por_kit" in kits.columns:
        catalogo["preco"] = pd.to_numeric(catalogo["preco"], errors="coerce").fillna(0.0)
        kits["qty_por_kit"] = pd.to_numeric(kits["qty_por_kit"], errors="coerce").fillna(0).astype(int)

    return kits, catalogo

# ---------------------------------------------------------
# UPLOADS
# ---------------------------------------------------------
def ler_full(file) -> pd.DataFrame:
    """ FULL (Magiic) — vendas por anúncio/SKU (últimos 60d). """
    if file is None:
        return pd.DataFrame(columns=["sku", "qtd"])
    df = pd.read_csv(file, sep=None, engine="python") if file.name.lower().endswith(".csv") else pd.read_excel(file)
    mapa = {
        "sku": ["sku", "codigo", "sku_anuncio", "sku produto", "id_sku", "anuncio_sku"],
        "qtd": ["qtd", "quantidade", "qtd_vendida", "vendido", "sold"]
    }
    df = _try_map_columns(df, mapa, ["sku", "qtd"], "FULL (Magiic)")
    df["sku"] = df["sku"].astype(str).str.strip()
    df["qtd"] = pd.to_numeric(df["qtd"], errors="coerce").fillna(0).astype(int)
    return df[["sku", "qtd"]]

def ler_estoque(file) -> pd.DataFrame:
    """ Estoque físico — saldo por SKU. """
    if file is None:
        return pd.DataFrame(columns=["sku", "estoque"])
    df = pd.read_csv(file, sep=None, engine="python") if file.name.lower().endswith(".csv") else pd.read_excel(file)
    mapa = {
        "sku": ["sku", "codigo", "id_sku", "produto"],
        "estoque": ["estoque", "saldo", "qtde", "quantidade", "stock"]
    }
    df = _try_map_columns(df, mapa, ["sku", "estoque"], "Estoque Físico")
    df["sku"] = df["sku"].astype(str).str.strip()
    df["estoque"] = pd.to_numeric(df["estoque"], errors="coerce").fillna(0).astype(int)
    return df[["sku", "estoque"]]

def ler_shopee(file) -> pd.DataFrame:
    """ Shopee/Mercado Turbo — vendas por SKU (opcional). """
    if file is None:
        return pd.DataFrame(columns=["sku", "qtd"])
    df = pd.read_csv(file, sep=None, engine="python") if file.name.lower().endswith(".csv") else pd.read_excel(file)
    mapa = {
        "sku": ["sku", "codigo", "id_sku", "produto"],
        "qtd": ["qtd", "quantidade", "vendido", "qty"]
    }
    df = _try_map_columns(df, mapa, ["sku", "qtd"], "Shopee/Mercado Turbo")
    df["sku"] = df["sku"].astype(str).str.strip()
    df["qtd"] = pd.to_numeric(df["qtd"], errors="coerce").fillna(0).astype(int)
    return df[["sku", "qtd"]]

# ---------------------------------------------------------
# CÁLCULO SIMPLIFICADO DE REPOSIÇÃO
# ---------------------------------------------------------
def calcular_compra(df_vendas: pd.DataFrame,
                    df_estoque: pd.DataFrame,
                    catalogo: pd.DataFrame,
                    horizonte_dias: int,
                    crescimento_pct: float,
                    lead_time_dias: int) -> pd.DataFrame:
    """
    Consumo médio diário = vendas_60d / 60
    Aplica crescimento (% ao mês)
    Estoque alvo = consumo * (horizonte + lead_time)
    Compra = max(alvo - estoque, 0)
    """
    vendas = df_vendas.groupby("sku", as_index=False)["qtd"].sum().rename(columns={"qtd": "vendas_60d"})
    estoque = df_estoque.groupby("sku", as_index=False)["estoque"].sum()
    base = pd.merge(catalogo, vendas, on="sku", how="left")
    base = pd.merge(base, estoque, on="sku", how="left")

    base["vendas_60d"] = base["vendas_60d"].fillna(0).astype(float)
    base["estoque"] = base["estoque"].fillna(0).astype(float)

    consumo_dia = base["vendas_60d"] / 60.0
    fator_cresc = (1.0 + (crescimento_pct / 100.0))
    consumo_dia_aj = consumo_dia * fator_cresc
    alvo = consumo_dia_aj * float(horizonte_dias + lead_time_dias)
    sugerir = np.maximum(alvo - base["estoque"], 0.0)

    base["consumo_dia"] = consumo_dia_aj.round(4)
    base["estoque_alvo"] = alvo.round(2)
    base["sugestao_compra"] = np.floor(sugerir).astype(int)
    base["custo_previsto"] = (base["sugestao_compra"] * base["preco"]).round(2)

    cols = ["sku", "fornecedor", "preco", "vendas_60d", "estoque", "consumo_dia",
            "estoque_alvo", "sugestao_compra", "custo_previsto"]
    return base[cols].sort_values(["fornecedor", "sku"]).reset_index(drop=True)

# =========================================================
# UI
# =========================================================
st.title("Reposição Logística — Alivvia")

# Parâmetros
with st.sidebar:
    st.header("Parâmetros")

    horizonte = st.number_input("Horizonte (dias)", min_value=1, max_value=180, value=60, step=1)
    crescimento = st.number_input("Crescimento % ao mês", min_value=-100.0, max_value=300.0, value=0.0, step=0.5)
    lead = st.number_input("Lead time (dias)", min_value=0, max_value=60, value=0, step=1)

    st.markdown("---")
    st.subheader("Padrão (KITS/CAT) do Google Sheets")
    # Planilha fixa (não edita ID)
    colb1, colb2 = st.columns([1,1])
    with colb1:
        st.markdown(
            f"[🔗 Abrir no Drive](https://docs.google.com/spreadsheets/d/{SHEET_ID_DEFAULT}/edit)",
            unsafe_allow_html=True
        )
    with colb2:
        if st.button("Recarregar padrão"):
            carregar_kits_catalogo.clear()

# Empresa (apenas visual)
empresa = st.radio("Empresa ativa", ["ALIVVIA", "JCA"], horizontal=True)

# Sempre carrega do Google Sheets FIXO
try:
    kits_df, cat_df = carregar_kits_catalogo(SHEET_ID_DEFAULT)
except Exception as e:
    st.error(f"Falha ao carregar o padrão do Google Sheets: {e}")
    st.stop()

# Uploads
st.subheader(f"Uploads — {empresa}")
col1, col2, col3 = st.columns(3)

with col1:
    st.caption("FULL (Magiic) — vendas por SKU")
    up_full = st.file_uploader("Arraste/Selecione (CSV/XLSX/XLS)", type=["csv", "xlsx", "xls"], key="full_up")

with col2:
    st.caption("Estoque Físico — saldo por SKU")
    up_estoque = st.file_uploader("Arraste/Selecione (CSV/XLSX/XLS)", type=["csv", "xlsx", "xls"], key="est_up")

with col3:
    st.caption("Shopee / Mercado Turbo — vendas por SKU (opcional)")
    up_shopee = st.file_uploader("Arraste/Selecione (CSV/XLSX/XLS)", type=["csv", "xlsx", "xls"], key="shop_up")

df_full = ler_full(up_full)
df_shop = ler_shopee(up_shopee)
df_estoque = ler_estoque(up_estoque)

# Consolida vendas
df_vendas = pd.concat([df_full, df_shop], ignore_index=True) if not df_shop.empty else df_full.copy()
if df_vendas.empty and df_estoque.empty:
    st.info("Envie pelo menos FULL (vendas) e Estoque para gerar a compra.")
    st.stop()

st.markdown("—")

# Botão gerar
if st.button(f"Gerar Compra — {empresa}"):
    try:
        resultado = calcular_compra(
            df_vendas=df_vendas,
            df_estoque=df_estoque,
            catalogo=cat_df,
            horizonte_dias=int(horizonte),
            crescimento_pct=float(crescimento),
            lead_time_dias=int(lead),
        )
    except KeyError as e:
        st.error(f"Erro de cabeçalho: {e}")
        st.stop()

    st.success("Compra gerada!")

    # Filtros: fornecedor e SKU (autocomplete)
    st.subheader("Filtros")
    fornecedores = sorted(resultado["fornecedor"].dropna().unique().tolist())
    f_sel = st.multiselect("Filtrar por fornecedor", fornecedores, placeholder="Escolha fornecedores…")

    skus = sorted(resultado["sku"].dropna().unique().tolist())
    sku_sel = st.multiselect("Buscar SKU(s)", skus, placeholder="Digite para buscar…")

    df_view = resultado.copy()
    if f_sel:
        df_view = df_view[df_view["fornecedor"].isin(f_sel)]
    if sku_sel:
        df_view = df_view[df_view["sku"].isin(sku_sel)]

    st.dataframe(df_view, use_container_width=True, hide_index=True)

    # Exportar CSV
    csv = df_view.to_csv(index=False).encode("utf-8-sig")
    st.download_button("Baixar CSV da compra", data=csv, file_name=f"compra_{empresa.lower()}.csv", mime="text/csv")
else:
    st.caption("Preencha os uploads e clique em **Gerar Compra**.")
