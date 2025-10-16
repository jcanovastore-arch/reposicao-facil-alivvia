# -*- coding: utf-8 -*-
import os
import io
import re
import time
from datetime import datetime

import numpy as np
import pandas as pd
import requests
import streamlit as st
from unidecode import unidecode

# =========================================================
# =================== CONFIG BÁSICA =======================
# =========================================================

st.set_page_config(
    page_title="Reposição Logística — Alivvia",
    layout="wide",
    page_icon="📦",
)

APP_TITLE = "Reposição Logística — Alivvia"
ARQ_PADRAO = "Padrao_produtos.xlsx"  # arquivo local priorizado
REQUIRED_CATALOGO_COLS = {"sku", "fornecedor", "preco"}  # colunas mínimas da aba CATALOGO
REQUIRED_KITS_COLS = {"kit_sku", "component_sku", "qty_por_kit"}  # colunas mínimas da aba KITS

# =========================================================
# ==================== FUNÇÕES BASE =======================
# =========================================================

def normaliza_txt(x: str) -> str:
    """Normaliza texto para comparações (sem acento, minúsculo, trim)."""
    if pd.isna(x):
        return ""
    return unidecode(str(x)).strip().lower()


def status_arquivo_local(caminho: str) -> str:
    """Retorna status amigável do arquivo local (existe/última modificação)."""
    if os.path.exists(caminho):
        ts = datetime.fromtimestamp(os.path.getmtime(caminho)).strftime("%Y-%m-%d %H:%M")
        return f"✅ Encontrado: **{caminho}** (modificado em {ts})"
    return f"❌ Não encontrado: **{caminho}**"


def extrai_id_planilha(valor: str) -> str | None:
    """
    Aceita:
      - ID puro: 1cTLARjq-xxxx
      - URL de edição do Sheets
    Retorna apenas o ID.
    """
    if not valor:
        return None
    # ID puro?
    if re.fullmatch(r"[A-Za-z0-9_\-]+", valor):
        return valor

    # Tenta pegar /d/<ID>/ do URL
    m = re.search(r"/d/([A-Za-z0-9_\-]+)/", valor)
    if m:
        return m.group(1)

    return None


def baixar_padrao_do_sheets(planilha_id: str, formato: str = "xlsx", gid: str | None = None) -> bytes:
    """
    Baixa a planilha do Google Sheets no formato desejado e retorna bytes do arquivo.
    - formato: 'xlsx' (padrão) ou 'csv'
    - gid: id da aba (opcional); se None e formato == xlsx, exporta o arquivo completo em xlsx.
    """
    if formato not in {"xlsx", "csv"}:
        raise ValueError("Formato inválido. Use 'xlsx' ou 'csv'.")

    if formato == "xlsx":
        url = f"https://docs.google.com/spreadsheets/d/{planilha_id}/export?format=xlsx"
    else:
        # CSV de uma aba específica
        if not gid:
            raise ValueError("Para format='csv', informe o gid da aba.")
        url = f"https://docs.google.com/spreadsheets/d/{planilha_id}/export?format=csv&gid={gid}"

    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return r.content


def carrega_catalogo_local(caminho_xlsx: str) -> tuple[pd.DataFrame, pd.DataFrame, list[str]]:
    """
    Lê o arquivo local Padrao_produtos.xlsx.
    Espera (se existirem) abas: 'CATALOGO' e 'KITS' (case-insensitive ok).
    Valida colunas mínimas. Retorna:
      - df_catalogo (pode vir vazio)
      - df_kits     (pode vir vazio)
      - warnings    (lista de mensagens amigáveis)
    """
    avisos = []
    if not os.path.exists(caminho_xlsx):
        avisos.append("Arquivo local de padrão não encontrado. Coloque **Padrao_produtos.xlsx** na pasta do app "
                      "ou use o botão 'Baixar padrão'.")
        return pd.DataFrame(), pd.DataFrame(), avisos

    try:
        xls = pd.ExcelFile(caminho_xlsx, engine="openpyxl")
        abas = {s.lower(): s for s in xls.sheet_names}
    except Exception as e:
        avisos.append(f"Falha ao abrir '{caminho_xlsx}': {e}")
        return pd.DataFrame(), pd.DataFrame(), avisos

    # Lê CATALOGO (se existir)
    df_catalogo = pd.DataFrame()
    if "catalogo" in abas:
        try:
            df_catalogo = pd.read_excel(xls, sheet_name=abas["catalogo"])
            # Normaliza colunas
            df_catalogo.columns = [normaliza_txt(c) for c in df_catalogo.columns]
            faltando = REQUIRED_CATALOGO_COLS - set(df_catalogo.columns)
            if faltando:
                avisos.append(f"Aba **CATALOGO** encontrada, mas faltam colunas: {sorted(faltando)}. "
                              f"Colunas atuais: {list(df_catalogo.columns)}")
                df_catalogo = pd.DataFrame()
            else:
                # Coerções mínimas
                df_catalogo["sku"] = df_catalogo["sku"].astype(str).str.strip()
                df_catalogo["fornecedor"] = df_catalogo["fornecedor"].astype(str).str.strip()
                # preço numérico
                df_catalogo["preco"] = pd.to_numeric(df_catalogo["preco"], errors="coerce").fillna(0.0)
        except Exception as e:
            avisos.append(f"Erro ao ler aba CATALOGO: {e}")
            df_catalogo = pd.DataFrame()
    else:
        avisos.append("Aba **CATALOGO** não encontrada (opcional, mas recomendada).")

    # Lê KITS (se existir)
    df_kits = pd.DataFrame()
    if "kits" in abas:
        try:
            df_kits = pd.read_excel(xls, sheet_name=abas["kits"])
            df_kits.columns = [normaliza_txt(c) for c in df_kits.columns]
            faltando_k = REQUIRED_KITS_COLS - set(df_kits.columns)
            if faltando_k:
                avisos.append(f"Aba **KITS** encontrada, mas faltam colunas: {sorted(faltando_k)}. "
                              f"Colunas atuais: {list(df_kits.columns)}")
                df_kits = pd.DataFrame()
            else:
                df_kits["kit_sku"] = df_kits["kit_sku"].astype(str).str.strip()
                df_kits["component_sku"] = df_kits["component_sku"].astype(str).str.strip()
                df_kits["qty_por_kit"] = pd.to_numeric(df_kits["qty_por_kit"], errors="coerce").fillna(0).astype(int)
        except Exception as e:
            avisos.append(f"Erro ao ler aba KITS: {e}")
            df_kits = pd.DataFrame()
    else:
        avisos.append("Aba **KITS** não encontrada (opcional, mas recomendada).")

    return df_catalogo, df_kits, avisos


def ler_upload(ufile, allow_excel=True) -> pd.DataFrame:
    """Lê um arquivo enviado (csv/xlsx/xls). Retorna DataFrame ou vazio."""
    if ufile is None:
        return pd.DataFrame()

    name = ufile.name.lower()
    try:
        if name.endswith(".csv"):
            return pd.read_csv(ufile)
        elif allow_excel and (name.endswith(".xlsx") or name.endswith(".xls")):
            return pd.read_excel(ufile)
        else:
            st.warning(f"Formato não suportado para '{ufile.name}'. Use CSV/XLSX/XLS.")
            return pd.DataFrame()
    except Exception as e:
        st.error(f"Falha ao ler '{ufile.name}': {e}")
        return pd.DataFrame()


def aplica_filtros(df: pd.DataFrame, fornecedor: str | None, sku_query: str | None) -> pd.DataFrame:
    """Filtra por fornecedor e SKU (se existirem). Não falha se colunas não existirem."""
    if df.empty:
        return df.copy()

    out = df.copy()
    cols = {c.lower(): c for c in df.columns}

    # por fornecedor
    if fornecedor and "fornecedor" in cols:
        out = out[out[cols["fornecedor"]].astype(str).str.strip() == fornecedor]

    # por sku (contém)
    if sku_query:
        key = None
        for cand in ["sku", "codigo", "cod", "id_sku"]:
            if cand in cols:
                key = cols[cand]
                break
        if key:
            q = normaliza_txt(sku_query)
            out = out[out[key].astype(str).map(normaliza_txt).str.contains(q, na=False)]

    return out


# =========================================================
# =============== PLACEHOLDER DE CÁLCULO ==================
# =========================================================
def calcular_compra(df_full: pd.DataFrame,
                    df_estoque: pd.DataFrame,
                    df_vendas: pd.DataFrame,
                    df_catalogo: pd.DataFrame,
                    horizonte_dias: int,
                    crescimento_pct: float,
                    leadtime_dias: int) -> pd.DataFrame:
    """
    Cálculo simplificado (placeholder, não mexe nos seus dados brutos).
    - Faz um merge leve por 'sku' se a coluna existir.
    - Estima 'qtd_sugerida' de forma simples para não travar sua operação.
    Você pode plugar aqui sua regra existente sem alterar o restante do app.
    """
    # Garante colunas 'sku'
    def ensure_sku(df):
        if df.empty:
            return df
        cols = [c for c in df.columns if normaliza_txt(c) == "sku"]
        if cols:
            if cols[0] != "sku":
                df = df.rename(columns={cols[0]: "sku"})
        else:
            # cria uma fake sku se não existir
            df = df.copy()
            df["sku"] = df.index.astype(str)
        return df

    df_full = ensure_sku(df_full)
    df_estoque = ensure_sku(df_estoque)
    df_vendas = ensure_sku(df_vendas)
    df_catalogo = ensure_sku(df_catalogo)

    # estoque atual (chuta coluna mais provável)
    estoque_cols = [c for c in df_estoque.columns if normaliza_txt(c) in {"estoque", "qty", "qtd", "saldo"}]
    df_est = df_estoque[["sku"] + estoque_cols].copy() if estoque_cols else df_estoque[["sku"]].copy()
    if estoque_cols:
        df_est = df_est.rename(columns={estoque_cols[0]: "estoque_atual"})
    else:
        df_est["estoque_atual"] = 0

    # vendas médias diárias (se houver algum campo de vendas)
    venda_cols = [c for c in df_vendas.columns if re.search(r"ven|qtd", c, flags=re.I)]
    df_ven = df_vendas[["sku"] + venda_cols].copy() if venda_cols else df_vendas[["sku"]].copy()
    if venda_cols:
        base = pd.to_numeric(df_ven[venda_cols[0]], errors="coerce").fillna(0)
        # converte para média diária aproximada (60 dias como base)
        mdd = base / 60.0
    else:
        mdd = pd.Series(0, index=df_ven.index)

    df_ven["media_dia"] = mdd

    # merge simples
    base = pd.merge(df_full, df_est[["sku", "estoque_atual"]], on="sku", how="left")
    base = pd.merge(base, df_ven[["sku", "media_dia"]], on="sku", how="left")
    base["estoque_atual"] = pd.to_numeric(base["estoque_atual"], errors="coerce").fillna(0)
    base["media_dia"] = pd.to_numeric(base["media_dia"], errors="coerce").fillna(0)

    # crescimento
    fator_crescimento = 1.0 + (crescimento_pct / 100.0)
    demanda_periodo = base["media_dia"] * float(horizonte_dias) * fator_crescimento

    # cobertura lead time
    demanda_lt = base["media_dia"] * float(leadtime_dias)

    # sugerida = demanda_periodo + demanda_lt - estoque
    base["qtd_sugerida"] = (demanda_periodo + demanda_lt - base["estoque_atual"]).round().astype(int)
    base.loc[base["qtd_sugerida"] < 0, "qtd_sugerida"] = 0

    # pega fornecedor/preço do catálogo (se existir)
    if not df_catalogo.empty and {"sku", "fornecedor"}.issubset(df_catalogo.columns):
        base = pd.merge(base, df_catalogo[["sku", "fornecedor"]], on="sku", how="left")

    if not df_catalogo.empty and {"sku", "preco"}.issubset(df_catalogo.columns):
        base = pd.merge(base, df_catalogo[["sku", "preco"]], on="sku", how="left")
        base["valor_total"] = base["qtd_sugerida"] * base["preco"]

    return base


# =========================================================
# ======================= UI / APP ========================
# =========================================================

st.title(APP_TITLE)

# ----------
# Parâmetros
# ----------
with st.sidebar:
    st.subheader("Parâmetros")
    horizonte = st.number_input("Horizonte (dias)", min_value=1, value=60, step=1)
    crescimento = st.number_input("Crescimento % ao mês", min_value=0.0, value=0.0, step=0.5, format="%.2f")
    leadtime = st.number_input("Lead time (dias)", min_value=0, value=0, step=1)

    st.markdown("---")
    st.subheader("Padrão (KITS/CAT) — opcional")
    st.caption("**Opção 1:** deixe o arquivo **Padrao_produtos.xlsx** na pasta do app (usado automaticamente).")
    st.caption("**Opção 2:** cole abaixo o **ID do Google Sheets** ou a **URL** de edição e clique em **Baixar padrão** "
               "(o arquivo será salvo localmente como **Padrao_produtos.xlsx**).")

    valor_sheets = st.text_input("ID do Sheets **ou** URL de edição (opcional)", value="", placeholder="1cTLARjq-... ou https://docs.google.com/spreadsheets/d/...")

    c1, c2 = st.columns([1, 1])
    with c1:
        if st.button("🔽 Baixar padrão (salvar local)"):
            _id = extrai_id_planilha(valor_sheets)
            if not _id:
                st.warning("Informe um **ID** ou **URL** válido do Google Sheets.")
            else:
                try:
                    # preferimos o XLSX completo
                    content = baixar_padrao_do_sheets(_id, formato="xlsx", gid=None)
                    with open(ARQ_PADRAO, "wb") as f:
                        f.write(content)
                    st.success(f"Padrão salvo como **{ARQ_PADRAO}**.")
                except Exception as e:
                    st.error(f"Falha ao baixar do Google Sheets: {e}")

    with c2:
        st.info(status_arquivo_local(ARQ_PADRAO))

    st.markdown("---")
    empresa = st.radio("Empresa ativa", options=["ALIVVIA", "JCA"], horizontal=True)

# -----------------
# Carrega Catálogo
# -----------------
df_catalogo, df_kits, avisos_cat = carrega_catalogo_local(ARQ_PADRAO)
if avisos_cat:
    for a in avisos_cat:
        st.warning(a)
else:
    st.success("Padrão carregado com sucesso.")

# -----------------
# Uploads principais
# -----------------
st.subheader("Uploads — " + empresa)

c_full, c_est, c_ven = st.columns(3)

with c_full:
    st.markdown("**FULL (Magic)**")
    up_full = st.file_uploader("Arraste/Selecione (CSV/XLSX/XLS)", key=f"full_{empresa}", type=["csv", "xlsx", "xls"])
with c_est:
    st.markdown("**Estoque Físico (CSV/XLSX/XLS)**")
    up_est = st.file_uploader("Arraste/Selecione", key=f"est_{empresa}", type=["csv", "xlsx", "xls"])
with c_ven:
    st.markdown("**Shopee / Mercado Turbo (vendas por SKU)**")
    up_ven = st.file_uploader("Arraste/Selecione", key=f"ven_{empresa}", type=["csv", "xlsx", "xls"])

df_full = ler_upload(up_full)
df_estoque = ler_upload(up_est)
df_vendas = ler_upload(up_ven)

# Filtros (fornecedor/SKU) – aplicados depois do cálculo
st.markdown("---")
st.subheader("Filtros (aplicados no resultado)")
f1, f2 = st.columns([1, 1])

fornecedor_filtro = None
sku_filtro = None

if not df_catalogo.empty and "fornecedor" in df_catalogo.columns:
    fornecedores = sorted(df_catalogo["fornecedor"].dropna().astype(str).unique().tolist())
    fornecedores = ["(todos)"] + fornecedores
    with f1:
        sel = st.selectbox("Fornecedor", fornecedores, index=0)
        fornecedor_filtro = None if sel == "(todos)" else sel
else:
    with f1:
        st.caption("Fornecedor: catálogo não disponível ou sem coluna 'fornecedor'.")

with f2:
    sku_filtro = st.text_input("SKU (opcional)", value="", placeholder="digite parte do SKU para filtrar…").strip()
    if sku_filtro == "":
        sku_filtro = None

# -----------------
# Botão principal
# -----------------
st.markdown("---")
if st.button(f"Gerar Compra — {empresa}", type="primary"):
    if df_full.empty:
        st.error("Envie o arquivo **FULL (Magic)**.")
    elif df_estoque.empty:
        st.error("Envie o arquivo **Estoque Físico**.")
    elif df_vendas.empty:
        st.error("Envie o arquivo de **Vendas (Shopee/MT)**.")
    else:
        with st.spinner("Calculando…"):
            base = calcular_compra(
                df_full=df_full,
                df_estoque=df_estoque,
                df_vendas=df_vendas,
                df_catalogo=df_catalogo,
                horizonte_dias=int(horizonte),
                crescimento_pct=float(crescimento),
                leadtime_dias=int(leadtime),
            )

            # aplica filtros na visualização
            base_f = aplica_filtros(base, fornecedor_filtro, sku_filtro)

        st.success(f"Compra gerada ({len(base_f)} linhas). Abaixo o resultado filtrado (se aplicável).")

        st.dataframe(base_f, use_container_width=True)

        # Exportar Excel
        export_name = f"compra_{empresa.lower()}_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
        with io.BytesIO() as output:
            with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
                base_f.to_excel(writer, index=False, sheet_name="compra")
            data = output.getvalue()

        st.download_button(
            label=f"⬇️ Baixar Excel ({export_name})",
            data=data,
            file_name=export_name,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            type="primary",
        )

# Rodapé
st.markdown("---")
st.caption("© Alivvia — simples, robusto e auditável.")
