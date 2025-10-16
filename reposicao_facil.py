# -*- coding: utf-8 -*-
import io
import os
import math
import time
import zipfile
import requests
import numpy as np
import pandas as pd
import streamlit as st

# =============== CONFIGS RÁPIDAS ===============
# ID da sua planilha de Padrão (KITS/CAT) no Google Sheets (a MESMA que você vem usando)
GSHEET_ID_PADRAO = "1cTLARjq-B5g50dL6tcntg7lb_Iu0ta43"  # troque se quiser
# Nome do arquivo local padrão (se existir na pasta, será usado)
PADRAO_LOCAL = "Padrao_produtos.xlsx"
# =============================================

st.set_page_config(page_title="Reposição Logística — Alivvia", layout="wide")

# ---------- helpers ----------
def read_any_table(file, **kwargs):
    """Lê CSV/XLS/XLSX automaticamente."""
    name = file.name if hasattr(file, "name") else str(file)
    lower = name.lower()
    if lower.endswith(".csv"):
        return pd.read_csv(file, **kwargs)
    elif lower.endswith(".xlsx") or lower.endswith(".xls"):
        return pd.read_excel(file, **kwargs)
    else:
        # tenta Excel por padrão
        try:
            return pd.read_excel(file, **kwargs)
        except Exception:
            return pd.read_csv(file, **kwargs)

def to_number(x):
    try:
        if pd.isna(x): 
            return 0.0
        s = str(x).replace(".", "").replace(",", ".")
        return float(s)
    except Exception:
        try:
            return float(x)
        except Exception:
            return 0.0

def baixar_padroes_do_drive(gsheet_id: str) -> bytes:
    """
    Baixa a planilha Google Sheets (arquivo inteiro) via export endpoint em XLSX.
    """
    export_url = f"https://docs.google.com/spreadsheets/d/{gsheet_id}/export?format=xlsx"
    r = requests.get(export_url, timeout=30)
    r.raise_for_status()
    return r.content

def garantir_padroes_em_memoria():
    """
    Tenta ler o padrão (KITS/CAT) local; se não tiver, usa o de sessão;
    se não tiver também, tenta baixar do Google.
    """
    # 1) Local
    if os.path.exists(PADRAO_LOCAL):
        try:
            xf = pd.ExcelFile(PADRAO_LOCAL)
            return xf
        except Exception:
            pass

    # 2) Sessão (em bytes)
    if "padrao_bytes" in st.session_state and st.session_state["padrao_bytes"]:
        try:
            bio = io.BytesIO(st.session_state["padrao_bytes"])
            xf = pd.ExcelFile(bio)
            return xf
        except Exception:
            pass

    # 3) Google (download)
    try:
        content = baixar_padroes_do_drive(st.session_state.get("PADRAO_GSHEET_ID", GSHEET_ID_PADRAO))
        st.session_state["padrao_bytes"] = content
        return pd.ExcelFile(io.BytesIO(content))
    except Exception as e:
        st.info("Use o botão **Baixar padrão** ou mantenha um `Padrao_produtos.xlsx` na mesma pasta do app.")
        return None

def montar_padroes(xf: pd.ExcelFile):
    """
    Lê as abas KITS e CATALOGO_SIMPLES (se existirem) do padrão.
    """
    kits = None
    cat  = None
    for name in xf.sheet_names:
        lname = name.strip().lower()
        if "kit" in lname:
            try:
                kits = xf.parse(name)
            except Exception:
                pass
        if "catalogo" in lname or "catálogo" in lname or "catalogo_simples" in lname:
            try:
                cat = xf.parse(name)
            except Exception:
                pass
    return kits, cat

def padronizar_catalogo(cat: pd.DataFrame) -> pd.DataFrame:
    """
    Tenta padronizar catálogo mínimo: sku, fornecedor, preco (se existir).
    """
    if cat is None or cat.empty:
        return pd.DataFrame(columns=["sku", "fornecedor"])

    df = cat.copy()
    cols = [c.lower() for c in df.columns]
    df.columns = cols

    # tenta encontrar colunas prováveis
    # sku
    if "sku" not in df.columns:
        # tenta synonyms
        for c in ["codigo", "código", "produto", "sku_id", "id"]:
            if c in df.columns:
                df["sku"] = df[c].astype(str)
                break
    else:
        df["sku"] = df["sku"].astype(str)

    # fornecedor
    if "fornecedor" not in df.columns:
        for c in ["vendor", "fabricante", "marca"]:
            if c in df.columns:
                df["fornecedor"] = df[c].astype(str)
                break
    if "fornecedor" not in df.columns:
        df["fornecedor"] = ""

    # preço (opcional)
    if "preco" not in df.columns:
        for c in ["preço", "price", "valor", "vl"]:
            if c in df.columns:
                df["preco"] = df[c].apply(to_number)
                break
    if "preco" not in df.columns:
        df["preco"] = np.nan

    return df[["sku", "fornecedor", "preco"]].copy()

def padronizar_estoque(df: pd.DataFrame) -> pd.DataFrame:
    """
    Espera coluna 'sku' e 'estoque' (ou parecidas).
    """
    if df is None or df.empty:
        return pd.DataFrame(columns=["sku", "estoque"])

    d = df.copy()
    d.columns = [c.lower() for c in d.columns]

    # sku
    if "sku" not in d.columns:
        for c in ["codigo", "código", "produto", "id", "ean"]:
            if c in d.columns:
                d["sku"] = d[c].astype(str)
                break
    else:
        d["sku"] = d["sku"].astype(str)

    # estoque
    if "estoque" not in d.columns:
        for c in ["qtd", "quantidade", "saldo", "stock", "qty"]:
            if c in d.columns:
                d["estoque"] = d[c].apply(to_number)
                break
    if "estoque" not in d.columns:
        d["estoque"] = 0

    return d[["sku", "estoque"]].groupby("sku", as_index=False)["estoque"].sum()

def padronizar_vendas(df: pd.DataFrame) -> pd.DataFrame:
    """
    Espera 'sku' e 'vendas_60d' (ou algo convertível).
    """
    if df is None or df.empty:
        return pd.DataFrame(columns=["sku", "vendas_60d"])

    d = df.copy()
    d.columns = [c.lower() for c in d.columns]

    if "sku" not in d.columns:
        for c in ["codigo", "código", "produto", "id", "ean"]:
            if c in d.columns:
                d["sku"] = d[c].astype(str)
                break
    else:
        d["sku"] = d["sku"].astype(str)

    # tenta vendas no horizonte 60d
    vcol = None
    for c in ["vendas_60d", "vendas60", "vendas", "qty_sold", "qtd_vendida", "vendidos"]:
        if c in d.columns:
            vcol = c
            break

    if vcol is None:
        # se tiver datas, etc… mas vamos só pôr zeros
        d["vendas_60d"] = 0.0
    else:
        d["vendas_60d"] = d[vcol].apply(to_number)

    return d[["sku", "vendas_60d"]].groupby("sku", as_index=False)["vendas_60d"].sum()

def calcular_compra(estoque: pd.DataFrame,
                    vendas: pd.DataFrame,
                    catalogo: pd.DataFrame,
                    horizonte_dias: int,
                    crescimento_pct: float,
                    leadtime_dias: int):
    """
    Regra simples:
    demanda = (vendas_60d/60) * horizonte * (1+%crescimento) - estoque
    compra = max(ceil(demanda), 0)
    """
    base = pd.merge(catalogo, estoque, on="sku", how="left")
    base = pd.merge(base, vendas, on="sku", how="left")

    base["estoque"]   = base["estoque"].fillna(0.0)
    base["vendas_60d"] = base["vendas_60d"].fillna(0.0)

    # previsão simples
    vendas_diarias = base["vendas_60d"] / 60.0
    fator = 1.0 + (crescimento_pct / 100.0)
    demanda = vendas_diarias * float(horizonte_dias + leadtime_dias) * fator

    base["demanda"] = demanda
    base["compra_sugerida"] = np.maximum(np.ceil(base["demanda"] - base["estoque"]), 0).astype(int)

    cols_ordem = ["sku", "fornecedor", "preco", "estoque", "vendas_60d", "demanda", "compra_sugerida"]
    for c in cols_ordem:
        if c not in base.columns:
            base[c] = np.nan
    base = base[cols_ordem].copy()

    # totas simplórios por fornecedor já saem via filtro; não exibimos resumo
    return base


# ---------- SIDEBAR ----------
with st.sidebar:
    st.header("Parâmetros")

    horizonte = st.number_input("Horizonte (dias)", min_value=7, max_value=120, value=60, step=1)
    cresc_pct = st.number_input("Crescimento % ao mês", min_value=-100.0, max_value=500.0, value=0.0, step=0.5, format="%.2f")
    leadtime  = st.number_input("Lead time (dias)", min_value=0, max_value=60, value=0, step=1)

    st.markdown("**Arquivo local opcional:**")
    if os.path.exists(PADRAO_LOCAL):
        st.success(f"{PADRAO_LOCAL}", icon="📄")
    else:
        st.info(f"Se existir `{PADRAO_LOCAL}` na pasta, será usado como padrão.", icon="ℹ️")

    st.markdown("**Padrão (KITS/CAT) por link**")

    # Guardamos o ID no estado (para você não precisar digitar sempre)
    st.session_state.setdefault("PADRAO_GSHEET_ID", GSHEET_ID_PADRAO)

    st.text_input("ID da planilha (Google Sheets):",
                  value=st.session_state["PADRAO_GSHEET_ID"],
                  key="PADRAO_GSHEET_ID")

    cols_btn = st.columns([1, 1])
    with cols_btn[0]:
        if st.button("🔗 Abrir no Drive (editar)"):
            url_edit = f"https://docs.google.com/spreadsheets/d/{st.session_state['PADRAO_GSHEET_ID']}/edit#gid=0"
            st.markdown(f"[Abrir planilha no Drive]({url_edit})")
    with cols_btn[1]:
        if st.button("⬇️ Baixar padrão"):
            try:
                content = baixar_padroes_do_drive(st.session_state["PADRAO_GSHEET_ID"])
                st.session_state["padrao_bytes"] = content
                st.success("Padrão baixado com sucesso em memória!")
            except Exception as e:
                st.error(f"Falha ao baixar: {e}")

# ---------- TÍTULO ----------
st.title("Reposição Logística — Alivvia")
st.caption("FULL por anúncio; compra por componente; Shopee explode antes; painel de estoques; prévia por SKU; filtro por fornecedor. Resultados ficam em memória (sem recálculo automático).")

# ---------- UPLOADS ----------
st.subheader("Uploads")
col1, col2, col3 = st.columns(3)

with col1:
    st.markdown("**FULL (Magiic)**")
    up_full = st.file_uploader("Arraste/Selecione (CSV/XLSX/XLS)", type=["csv", "xlsx", "xls"], key="fullu")

with col2:
    st.markdown("**Estoque Físico (CSV/XLSX/XLS)**")
    up_estoque = st.file_uploader("Arraste/Selecione (CSV/XLSX/XLS)", type=["csv", "xlsx", "xls"], key="estq")

with col3:
    st.markdown("**Shopee / Mercado Turbo (vendas por SKU)**")
    up_vendas = st.file_uploader("Arraste/Selecione (CSV/XLSX/XLS)", type=["csv", "xlsx", "xls"], key="vend")

# ---------- LEITURA PADRÃO ----------
xf = garantir_padroes_em_memoria()
kits, cat = (None, None)
if xf is not None:
    try:
        kits, cat = montar_padroes(xf)
    except Exception:
        kits, cat = (None, None)

catalogo = padronizar_catalogo(cat)

# ---------- LEITURA FULL/ESTOQUE/VENDAS ----------
df_estq = pd.DataFrame(columns=["sku", "estoque"])
df_vend = pd.DataFrame(columns=["sku", "vendas_60d"])

if up_estoque is not None:
    try:
        df_estq = padronizar_estoque(read_any_table(up_estoque))
    except Exception as e:
        st.warning(f"Não consegui ler ESTOQUE: {e}")

if up_vendas is not None:
    try:
        df_vend = padronizar_vendas(read_any_table(up_vendas))
    except Exception as e:
        st.warning(f"Não consegui ler VENDAS: {e}")

# FULL não é obrigatório aqui (muitos times não precisam dele para o cálculo simples).
# Se quiser, você pode usar o FULL para enriquecer, mas vamos ignorar por simplicidade robusta.

# ---------- CÁLCULO ----------
if catalogo is None or catalogo.empty:
    st.error("Catálogo do padrão vazio/ausente. Abra/baixe o padrão e tente novamente.")
    st.stop()

df_final = calcular_compra(
    estoque=df_estq,
    vendas=df_vend,
    catalogo=catalogo,
    horizonte_dias=int(horizonte),
    crescimento_pct=float(cresc_pct),
    leadtime_dias=int(leadtime),
)

# ---------- FILTROS (Fornecedor / SKU) ----------
st.subheader("Filtros")

colf1, colf2 = st.columns([1, 1])

with colf1:
    fornecedores = sorted(df_final["fornecedor"].fillna("").astype(str).unique(), key=lambda s: s.lower())
    sel_fornec = st.multiselect("Filtrar por fornecedor (opcional)", options=fornecedores, default=[])

with colf2:
    skus = sorted(df_final["sku"].fillna("").astype(str).unique(), key=lambda s: s.lower())
    sel_skus = st.multiselect("Filtrar por SKU (opcional)", options=skus, default=[])

df_view = df_final.copy()
if sel_fornec:
    df_view = df_view[df_view["fornecedor"].astype(str).isin(sel_fornec)]
if sel_skus:
    df_view = df_view[df_view["sku"].astype(str).isin(sel_skus)]

# ---------- PRÉVIA (tabela principal) ----------
st.subheader("Prévia (após filtros)")
st.dataframe(
    df_view.style.format({
        "preco": "R$ {:.2f}",
        "estoque": "{:.0f}",
        "vendas_60d": "{:.0f}",
        "demanda": "{:.1f}",
        "compra_sugerida": "{:.0f}",
    }),
    use_container_width=True,
    height=480
)

# ---------- EXPORT ----------
st.markdown("---")
colx1, colx2 = st.columns([1, 1])
with colx1:
    st.download_button(
        "💾 Baixar compra (CSV)",
        data=df_view.to_csv(index=False).encode("utf-8"),
        file_name="compra_sugerida.csv",
        mime="text/csv",
    )
with colx2:
    st.download_button(
        "📗 Baixar compra (XLSX)",
        data=lambda: _to_xlsx_bytes(df_view),
        file_name="compra_sugerida.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

# util p/ xlsx
def _to_xlsx_bytes(df: pd.DataFrame) -> bytes:
    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="xlsxwriter") as w:
        df.to_excel(w, index=False, sheet_name="Compra")
        w.book.close()
    bio.seek(0)
    return bio.getvalue()
