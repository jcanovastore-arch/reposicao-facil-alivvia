# reposicao_facil.py
import io
import time
import requests
import pandas as pd
import streamlit as st

# ========== CONFIGURE AQUI (Google Sheets) ==========
SHEET_ID = "1cTLARjq-B5g50dL6tcntg7lb_Iu0ta43"  # <-- ID da sua planilha
GID_KITS = "1589453187"                         # <-- gid da aba KITS (você já passou)
GID_CATALOGO = "0"                              # <-- gid da aba CATALOGO (troque pelo gid correto)
# ====================================================

st.set_page_config(page_title="Reposição Logística — Alivvia", layout="wide")

# ---------- util ----------
def gs_export_csv_url(sheet_id: str, gid: str) -> str:
    return f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"

def read_gs_csv(sheet_id: str, gid: str) -> pd.DataFrame:
    url = gs_export_csv_url(sheet_id, gid)
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return pd.read_csv(io.BytesIO(r.content))

def normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = (
        df.columns.str.strip()
        .str.lower()
        .str.replace(r"\s+", "_", regex=True)
        .str.replace("ã", "a")
        .str.replace("ç", "c")
        .str.replace("á", "a")
        .str.replace("é", "e")
        .str.replace("í", "i")
        .str.replace("ó", "o")
        .str.replace("ú", "u")
    )
    return df

def exige_colunas(df: pd.DataFrame, obrig: list, nome_tabela: str):
    faltam = [c for c in obrig if c not in df.columns]
    if faltam:
        raise ValueError(
            f"Colunas obrigatórias ausentes em {nome_tabela}: {faltam}\n"
            f"Colunas lidas: {list(df.columns)}"
        )

# ---------- carrega padrão do Google ----------
@st.cache_data(ttl=300, show_spinner=False)
def load_gs_data() -> dict:
    # KITS
    kits = read_gs_csv(SHEET_ID, GID_KITS)
    kits = normalize_cols(kits)
    # aceitamos variações usuais:
    possiveis_kits = {
        "kit_sku": ["kit_sku", "kit", "sku_kit"],
        "component_sku": ["component_sku", "componente", "sku_componente", "component"],
        "qty_por_kit": ["qty_por_kit", "qtd_por_kit", "quantidade_por_kit", "qtd"]
    }
    rename_k = {}
    for alvo, candidatas in possiveis_kits.items():
        for c in candidatas:
            if c in kits.columns:
                rename_k[c] = alvo
                break
    kits = kits.rename(columns=rename_k)
    exige_colunas(kits, ["kit_sku", "component_sku", "qty_por_kit"], "KITS")

    # CATALOGO
    catalogo = read_gs_csv(SHEET_ID, GID_CATALOGO)
    catalogo = normalize_cols(catalogo)
    possiveis_cat = {
        "sku": ["sku", "produto", "item", "codigo"],
        "fornecedor": ["fornecedor", "supplier", "fab", "marca"],
        "preco": ["preco", "preço", "price", "valor"]
    }
    rename_c = {}
    for alvo, candidatas in possiveis_cat.items():
        for c in candidatas:
            if c in catalogo.columns:
                rename_c[c] = alvo
                break
    catalogo = catalogo.rename(columns=rename_c)
    exige_colunas(catalogo, ["sku", "fornecedor", "preco"], "CATALOGO")

    # remove duplicatas de catálogo (última vence)
    catalogo = catalogo.drop_duplicates(subset=["sku"], keep="last")

    return {"kits": kits, "catalogo": catalogo}

# ---------- UI ----------
with st.sidebar:
    st.header("Parâmetros")
    horizonte = st.number_input("Horizonte (dias)", min_value=0, value=60, step=1)
    crescimento = st.number_input("Crescimento % ao mês", min_value=0.0, value=0.0, step=0.5, format="%.2f")
    lead = st.number_input("Lead time (dias)", min_value=0, value=0, step=1)

    st.markdown("---")
    st.subheader("Padrão (KITS/CAT) do Google Sheets")

    abrir_url = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/edit"
    st.link_button("🔗 Abrir no Drive (editar)", abrir_url)

    if st.button("🔄 Recarregar padrão"):
        load_gs_data.clear()
        st.success("Padrão recarregado do Google Sheets.")
        st.experimental_rerun()

st.title("Reposição Logística — Alivvia")

# Empresa ativa
empresa = st.radio("Empresa ativa", ["ALIVVIA", "JCA"], horizontal=True)

# Tenta carregar padrão
try:
    data = load_gs_data()
    KITS = data["kits"]
    CATALOGO = data["catalogo"]
except Exception as e:
    st.error(f"Falha ao carregar o padrão do Google Sheets:\n\n{e}")
    st.stop()

# ---------- Uploads ----------
st.subheader("Uploads")
col1, col2, col3 = st.columns(3)
with col1:
    st.markdown("**FULL (Magic)**")
    full_file = st.file_uploader("Arraste/Selecione (CSV/XLSX/XLS)", type=["csv", "xlsx", "xls"], key="full")

with col2:
    st.markdown("**Estoque Físico (CSV/XLSX/XLS)**")
    estoque_file = st.file_uploader("Arraste/Selecione (CSV/XLSX/XLS)", type=["csv", "xlsx", "xls"], key="estoque")

with col3:
    st.markdown("**Shopee / Mercado Turbo (vendas por SKU)**")
    vendas_file = st.file_uploader("Arraste/Selecione (CSV/XLSX/XLS)", type=["csv", "xlsx", "xls"], key="vendas")

# ---------- filtros (Fornecedor e SKU) ----------
st.markdown("---")
st.subheader("Filtros")

# Fornecedores do catálogo (alfabético, únicos)
fornecedores_lista = sorted([f for f in CATALOGO["fornecedor"].dropna().unique().tolist() if str(f).strip() != ""])
fornecedores_sel = st.multiselect("Filtrar por Fornecedor", options=fornecedores_lista)

# Autocomplete SKU (com base no catálogo)
sku_opcoes = CATALOGO["sku"].dropna().astype(str).sort_values().unique().tolist()
sku_sel = st.multiselect("Filtrar por SKU (busca)", options=sku_opcoes)

# Filtra catálogo conforme seleção
catalogo_filtrado = CATALOGO.copy()
if fornecedores_sel:
    catalogo_filtrado = catalogo_filtrado[catalogo_filtrado["fornecedor"].isin(fornecedores_sel)]
if sku_sel:
    catalogo_filtrado = catalogo_filtrado[catalogo_filtrado["sku"].astype(str).isin(sku_sel)]

# Mostra catálogo filtrado de referência
with st.expander("📄 Catálogo (após filtros) — referência", expanded=False):
    st.dataframe(catalogo_filtrado, use_container_width=True, height=260)

# ---------- Nota de cálculo ----------
st.caption("FULL por anúncio; compra por componente; Shopee explode antes; painel de estoques; prévia por SKU; filtro por fornecedor. Resultados ficam em memória (sem recalculo automático).")

# ---------- Botão de gerar compra (placeholder de cálculo) ----------
def ler_planilha(file):
    if file is None:
        return None
    name = file.name.lower()
    if name.endswith(".csv"):
        return pd.read_csv(file)
    else:
        return pd.read_excel(file)

st.markdown("---")
if st.button(f"Gerar Compra — {empresa}"):
    # Aqui você encaixa o seu cálculo real usando:
    # - KITS
    # - catalogo_filtrado (já filtrado por fornecedor e/ou sku)
    # - full_df, estoque_df, vendas_df (se enviados)
    # - horizonte, crescimento, lead, empresa
    full_df = ler_planilha(full_file)
    estoque_df = ler_planilha(estoque_file)
    vendas_df = ler_planilha(vendas_file)

    # checagens simples
    if full_df is None or estoque_df is None:
        st.warning("Envie pelo menos FULL e ESTOQUE para gerar a compra.")
        st.stop()

    # --------- EXEMPLO DE PRÉVIA (placeholder) -----------
    # Este trecho NÃO é o seu cálculo final; é só para
    # você ver que filtros estão funcionando e que os dados entram.
    st.success("Dados recebidos. (Cálculo final aqui)")

    st.write("**KITS (amostra):**")
    st.dataframe(KITS.head(), use_container_width=True)

    st.write("**CATÁLOGO (filtrado) — amostra:**")
    st.dataframe(catalogo_filtrado.head(), use_container_width=True)

    st.write("**FULL (amostra):**")
    st.dataframe(full_df.head(), use_container_width=True)

    st.write("**ESTOQUE (amostra):**")
    st.dataframe(estoque_df.head(), use_container_width=True)

    if vendas_df is not None:
        st.write("**VENDAS (amostra):**")
        st.dataframe(vendas_df.head(), use_container_width=True)

else:
    st.info("Ajuste filtros (Fornecedor/SKU), envie os arquivos e clique em **Gerar Compra**.")
