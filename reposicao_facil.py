# Reposição Logística — Alivvia (Streamlit)
# Multi-empresa (ALIVVIA/JCA), padrão KITS/CAT direto do Google Sheets via Secrets,
# FULL por anúncio; compra por componente; Shopee explode antes; painel de estoques;
# prévia por SKU; filtro por fornecedor. Resultados ficam em memória por empresa.

import os
import io
import re
import hashlib
import datetime as dt
from dataclasses import dataclass

import numpy as np
import pandas as pd
from unidecode import unidecode
import streamlit as st
import requests

# ===================== Utils =====================
def br_to_float(x):
    if pd.isna(x):
        return np.nan
    if isinstance(x, (int, float, np.integer, np.floating)):
        return float(x)
    s = str(x).strip()
    if s == "":
        return np.nan
    s = s.replace("\u00a0", " ")
    s = s.replace("R$", "").replace(" ", "")
    s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except Exception:
        return np.nan

def norm_header(s: str) -> str:
    s = (s or "").strip().lower()
    s = unidecode(s)
    for ch in [" ", "-", "(", ")", "/", "\\", "[", "]", "."]:
        s = s.replace(ch, "_")
    while "__" in s:
        s = s.replace("__", "_")
    return s.strip("_")

def norm_sku(x: str) -> str:
    if pd.isna(x):
        return ""
    return unidecode(str(x)).strip().upper()

@dataclass
class Catalogo:
    catalogo_simples: pd.DataFrame
    kits_reais: pd.DataFrame

# ===================== Padrão produtos (KITS + CAT) =====================
def carregar_padrao_produtos(caminho_ou_buffer) -> Catalogo:
    try:
        xls = pd.ExcelFile(caminho_ou_buffer)
    except Exception as e:
        raise RuntimeError(f"Não consegui abrir '{getattr(caminho_ou_buffer, 'name', 'buffer')}'. Erro: {e}")

    def load_sheet(opts):
        for n in opts:
            if n in xls.sheet_names:
                return pd.read_excel(xls, n)
        raise RuntimeError(f"Aba não encontrada. Esperado uma de {opts}.")

    df_kits = load_sheet(["KITS", "KITS_REAIS", "kits", "kits_reais"]).copy()
    df_cat  = load_sheet(["CATALOGO_SIMPLES", "CATALOGO", "catalogo_simples", "catalogo"]).copy()

    df_kits.columns = [norm_header(c) for c in df_kits.columns]
    if "qty" not in df_kits.columns and "qty_por_kit" not in df_kits.columns:
        raise RuntimeError("Na KITS: precisa de 'kit_sku', 'component_sku', 'qty(>=1)'.")
    if "qty" not in df_kits.columns:
        df_kits["qty"] = df_kits.get("qty_por_kit", 1)
    for c in ["kit_sku", "component_sku", "qty"]:
        if c not in df_kits.columns:
            raise RuntimeError("Na KITS: faltam colunas obrigatórias.")

    df_kits = df_kits[["kit_sku", "component_sku", "qty"]].copy()
    df_kits["kit_sku"]       = df_kits["kit_sku"].map(norm_sku)
    df_kits["component_sku"] = df_kits["component_sku"].map(norm_sku)
    df_kits["qty"]           = df_kits["qty"].map(br_to_float).fillna(0).astype(int)
    df_kits = df_kits[df_kits["qty"] >= 1].drop_duplicates(subset=["kit_sku","component_sku"], keep="first")

    df_cat.columns = [norm_header(c) for c in df_cat.columns]
    if "component_sku" not in df_cat.columns:
        raise RuntimeError("CATALOGO_SIMPLES precisa ter 'component_sku'.")
    if "fornecedor" not in df_cat.columns:        df_cat["fornecedor"] = ""
    if "status_reposicao" not in df_cat.columns:  df_cat["status_reposicao"] = ""
    df_cat["component_sku"]    = df_cat["component_sku"].map(norm_sku)
    df_cat["fornecedor"]       = df_cat["fornecedor"].fillna("").astype(str)
    df_cat["status_reposicao"] = df_cat["status_reposicao"].fillna("").astype(str)

    return Catalogo(catalogo_simples=df_cat, kits_reais=df_kits)

def construir_kits_efetivo(cat: Catalogo) -> pd.DataFrame:
    kits = cat.kits_reais.copy()
    existentes = set(kits["kit_sku"].unique())
    alias = []
    for s in cat.catalogo_simples["component_sku"].unique().tolist():
        s = norm_sku(s)
        if s and s not in existentes:
            alias.append((s, s, 1))
    if alias:
        kits = pd.concat([kits, pd.DataFrame(alias, columns=["kit_sku","component_sku","qty"])], ignore_index=True)
    return kits.drop_duplicates(subset=["kit_sku","component_sku"], keep="first")

# ===================== Leitura genérica =====================
def load_any_table(uploaded_file) -> pd.DataFrame:
    if uploaded_file is None:
        return None
    name = uploaded_file.name.lower()
    try:
        if name.endswith(".csv"):
            uploaded_file.seek(0)
            df = pd.read_csv(uploaded_file, dtype=str, keep_default_na=False, sep=None, engine="python")
        else:
            uploaded_file.seek(0)
            df = pd.read_excel(uploaded_file, dtype=str, keep_default_na=False)
    except Exception as e:
        raise RuntimeError(f"Não consegui ler '{uploaded_file.name}': {e}")

    df.columns = [norm_header(c) for c in df.columns]

    # FULL pode ter header na 3ª linha
    if ("sku" not in df.columns) and ("codigo" not in df.columns) and ("codigo_sku" not in df.columns) and len(df) > 0:
        try:
            uploaded_file.seek(0)
            df = pd.read_excel(uploaded_file, dtype=str, keep_default_na=False, header=2)
            df.columns = [norm_header(c) for c in df.columns]
        except Exception:
            pass

    cols = set(df.columns)
    sku_col = next((c for c in ["sku","codigo","codigo_sku"] if c in cols), None)
    if sku_col:
        df[sku_col] = df[sku_col].map(norm_sku)
        df = df[df[sku_col] != ""]
    for c in list(df.columns):
        df = df[~df[c].astype(str).str.contains(r"^TOTALS?$|^TOTAIS?$", case=False, na=False)]
    return df.reset_index(drop=True)

# ===================== Detecção por conteúdo =====================
def mapear_tipo(df: pd.DataFrame) -> str:
    cols = [c.lower() for c in df.columns]
    tem_vendas60 = any(c.startswith("vendas_60d") or c in {"vendas 60d","vendas_qtd_60d"} for c in cols)
    tem_estoque  = any(c in {"estoque_full","estoque_atual"} for c in cols)
    tem_transito = any(c in {"em_transito","em transito","em_transito_full","em_transito_do_anuncio"} for c in cols)
    tem_sku_std  = any(c in {"sku","codigo","codigo_sku"} for c in cols)
    if tem_sku_std and ((tem_vendas60 and tem_estoque) or (tem_vendas60 and tem_transito) or (tem_estoque and tem_transito)):
        return "FULL"

    tem_preco = any(c in {"preco","preco_compra","preco_medio","custo","custo_medio"} for c in cols)
    tem_estoque_fis = any(c in {"estoque_atual","qtd","quantidade"} for c in cols)
    tem_sku_livre = any("sku" in c for c in cols)
    if tem_sku_livre and tem_estoque_fis and tem_preco:
        return "FISICO"

    tem_qtd_livre = any(("qtde" in c) or ("quant" in c) or ("venda" in c) or ("order" in c) for c in cols)
    if tem_sku_livre and tem_qtd_livre and not tem_preco:
        return "VENDAS"

    return "DESCONHECIDO"

# ===================== Mapeamento de colunas =====================
def mapear_colunas(df: pd.DataFrame, tipo: str) -> pd.DataFrame:
    if tipo == "FULL":
        if "sku" in df.columns: df["SKU"] = df["sku"].map(norm_sku)
        elif "codigo" in df.columns: df["SKU"] = df["codigo"].map(norm_sku)
        elif "codigo_sku" in df.columns: df["SKU"] = df["codigo_sku"].map(norm_sku)
        else: raise RuntimeError("FULL inválido: precisa de coluna SKU/codigo.")
        c_v = [c for c in df.columns if c in ["vendas_qtd_60d","vendas_60d","vendas 60d"] or c.startswith("vendas_60d")]
        if not c_v: raise RuntimeError("FULL inválido: faltou Vendas_60d.")
        df["Vendas_Qtd_60d"] = df[c_v[0]].map(br_to_float).fillna(0).astype(int)
        c_e = [c for c in df.columns if c in ["estoque_full","estoque_atual"]]
        if not c_e: raise RuntimeError("FULL inválido: faltou Estoque_Full/estoque_atual.")
        df["Estoque_Full"] = df[c_e[0]].map(br_to_float).fillna(0).astype(int)
        c_t = [c for c in df.columns if c in ["em_transito","em transito","em_transito_full","em_transito_do_anuncio"]]
        df["Em_Transito"] = df[c_t[0]].map(br_to_float).fillna(0).astype(int) if c_t else 0
        return df[["SKU","Vendas_Qtd_60d","Estoque_Full","Em_Transito"]].copy()

    if tipo == "FISICO":
        sku_series = df.get("sku") or df.get("codigo") or df.get("codigo_sku")
        if sku_series is None:
            sku_series = df[next(c for c in df.columns if "sku" in c.lower())]
        df["SKU"] = sku_series.map(norm_sku)
        c_q = [c for c in df.columns if c in ["estoque_atual","qtd","quantidade"]]
        if not c_q: raise RuntimeError("FÍSICO inválido: faltou Estoque (estoque_atual/qtd/quantidade).")
        df["Estoque_Fisico"] = df[c_q[0]].map(br_to_float).fillna(0).astype(int)
        c_p = [c for c in df.columns if c in ["preco","preco_compra","custo","custo_medio","preco_medio"]]
        if not c_p: raise RuntimeError("FÍSICO inválido: faltou Preço/Custo.")
        df["Preco"] = df[c_p[0]].map(br_to_float).fillna(0.0)
        return df[["SKU","Estoque_Fisico","Preco"]].copy()

    if tipo == "VENDAS":
        sku_col = next((c for c in df.columns if "sku" in c.lower()), None)
        if sku_col is None: raise RuntimeError("VENDAS inválido: não achei coluna de SKU.")
        df["SKU"] = df[sku_col].map(norm_sku)
        cand_qty = []
        for c in df.columns:
            cl = c.lower(); score = 0
            if "qtde" in cl: score += 3
            if "quant" in cl: score += 2
            if "venda" in cl: score += 1
            if "order" in cl: score += 1
            if score > 0: cand_qty.append((score, c))
        if not cand_qty: raise RuntimeError("VENDAS inválido: não achei coluna de Quantidade.")
        cand_qty.sort(reverse=True); qcol = cand_qty[0][1]
        df["Quantidade"] = df[qcol].map(br_to_float).fillna(0).astype(int)
        return df[["SKU","Quantidade"]].copy()

    raise RuntimeError("Tipo de arquivo desconhecido.")

# ===================== Explosão e Cálculo =====================
def explodir_por_kits(df: pd.DataFrame, kits: pd.DataFrame, sku_col: str, qtd_col: str) -> pd.DataFrame:
    base = df.copy()
    base["kit_sku"] = base[sku_col].map(norm_sku)
    base["qtd"]     = base[qtd_col].astype(int)
    merged   = base.merge(kits, on="kit_sku", how="left")
    exploded = merged.dropna(subset=["component_sku"]).copy()
    exploded["qty"] = exploded["qty"].astype(int)
    exploded["quantidade_comp"] = exploded["qtd"] * exploded["qty"]
    out = exploded.groupby("component_sku", as_index=False)["quantidade_comp"].sum()
    return out.rename(columns={"component_sku":"SKU","quantidade_comp":"Quantidade"})

def calcular(full_df, fisico_df, vendas_df, cat: Catalogo, h=60, g=0.0, LT=0):
    kits = construir_kits_efetivo(cat)

    full = full_df.copy()
    full["SKU"]            = full["SKU"].map(norm_sku)
    full["Vendas_Qtd_60d"] = full["Vendas_Qtd_60d"].astype(int)
    full["Estoque_Full"]   = full["Estoque_Full"].astype(int)
    full["Em_Transito"]    = full["Em_Transito"].astype(int)

    shp = vendas_df.copy()
    shp["SKU"]            = shp["SKU"].map(norm_sku)
    shp["Quantidade_60d"] = shp["Quantidade"].astype(int)

    ml_comp = explodir_por_kits(
        full[["SKU","Vendas_Qtd_60d"]].rename(columns={"SKU":"kit_sku","Vendas_Qtd_60d":"Qtd"}),
        kits,"kit_sku","Qtd"
    ).rename(columns={"Quantidade":"ML_60d"})
    shopee_comp = explodir_por_kits(
        shp[["SKU","Quantidade_60d"]].rename(columns={"SKU":"kit_sku","Quantidade_60d":"Qtd"}),
        kits,"kit_sku","Qtd"
    ).rename(columns={"Quantidade":"Shopee_60d"})

    cat_df = cat.catalogo_simples[["component_sku","fornecedor","status_reposicao"]].rename(columns={"component_sku":"SKU"})
    demanda = cat_df.merge(ml_comp, on="SKU", how="left").merge(shopee_comp, on="SKU", how="left")
    demanda[["ML_60d","Shopee_60d"]] = demanda[["ML_60d","Shopee_60d"]].fillna(0).astype(int)
    demanda["TOTAL_60d"] = np.maximum(demanda["ML_60d"] + demanda["Shopee_60d"], demanda["ML_60d"]).astype(int)

    fis = fisico_df.copy()
    fis["SKU"]            = fis["SKU"].map(norm_sku)
    fis["Estoque_Fisico"] = fis["Estoque_Fisico"].fillna(0).astype(int)
    fis["Preco"]          = fis["Preco"].fillna(0.0)

    base = demanda.merge(fis, on="SKU", how="left")
    base["Estoque_Fisico"] = base["Estoque_Fisico"].fillna(0).astype(int)
    base["Preco"]          = base["Preco"].fillna(0.0)

    fator = (1.0 + g/100.0) ** (h/30.0)
    fk = full.copy()
    fk["vendas_dia"]     = fk["Vendas_Qtd_60d"] / 60.0
    fk["alvo"]           = np.round(fk["vendas_dia"] * (LT + h) * fator).astype(int)
    fk["oferta"]         = (fk["Estoque_Full"] + fk["Em_Transito"]).astype(int)
    fk["envio_desejado"] = (fk["alvo"] - fk["oferta"]).clip(lower=0).astype(int)

    necessidade = explodir_por_kits(
        fk[["SKU","envio_desejado"]].rename(columns={"SKU":"kit_sku","envio_desejado":"Qtd"}),
        kits,"kit_sku","Qtd"
    ).rename(columns={"Quantidade":"Necessidade"})

    base = base.merge(necessidade, on="SKU", how="left")
    base["Necessidade"] = base["Necessidade"].fillna(0).astype(int)

    base["Demanda_dia"]  = base["TOTAL_60d"] / 60.0
    base["Reserva_30d"]  = np.round(base["Demanda_dia"] * 30).astype(int)
    base["Folga_Fisico"] = (base["Estoque_Fisico"] - base["Reserva_30d"]).clip(lower=0).astype(int)
    base["Compra_Sugerida"] = (base["Necessidade"] - base["Folga_Fisico"]).clip(lower=0).astype(int)

    mask_nao = base["status_reposicao"].str.lower().str.contains("nao_repor", na=False)
    base.loc[mask_nao, "Compra_Sugerida"] = 0

    base["Valor_Compra_R$"] = (base["Compra_Sugerida"].astype(float) * base["Preco"].astype(float)).round(2)
    base["Vendas_h_ML"]     = np.round(base["ML_60d"] * (h/60.0)).astype(int)
    base["Vendas_h_Shopee"] = np.round(base["Shopee_60d"] * (h/60.0)).astype(int)

    base = base.sort_values(["fornecedor","Valor_Compra_R$","SKU"], ascending=[True, False, True])

    df_final = base[[
        "SKU","fornecedor",
        "Vendas_h_ML","Vendas_h_Shopee",
        "Estoque_Fisico","Preco","Compra_Sugerida","Valor_Compra_R$",
        "ML_60d","Shopee_60d","TOTAL_60d","Reserva_30d","Folga_Fisico","Necessidade"
    ]].reset_index(drop=True)

    fis_unid  = int(fis["Estoque_Fisico"].sum())
    fis_valor = float((fis["Estoque_Fisico"] * fis["Preco"]).sum())
    full_stock_comp = explodir_por_kits(
        full[["SKU","Estoque_Full"]].rename(columns={"SKU":"kit_sku","Estoque_Full":"Qtd"}),
        kits,"kit_sku","Qtd"
    ).merge(fis[["SKU","Preco"]], on="SKU", how="left")
    full_unid  = int(full["Estoque_Full"].sum())
    full_valor = float((full_stock_comp["Quantidade"].fillna(0) * full_stock_comp["Preco"].fillna(0.0)).sum())

    painel = {"full_unid": full_unid, "full_valor": full_valor, "fisico_unid": fis_unid, "fisico_valor": fis_valor}
    return df_final, painel

# ===================== Export XLSX =====================
def sha256_of_csv(df: pd.DataFrame) -> str:
    return hashlib.sha256(df.to_csv(index=False).encode("utf-8")).hexdigest()

def exportar_xlsx(df_final: pd.DataFrame, h: int, params: dict, pendencias: list | None = None) -> bytes:
    for c in ["Vendas_h_ML","Vendas_h_Shopee","Estoque_Fisico","Compra_Sugerida","Reserva_30d","Folga_Fisico","Necessidade","ML_60d","Shopee_60d","TOTAL_60d"]:
        if (df_final[c] < 0).any() or (df_final[c].astype(float) % 1 != 0).any():
            raise RuntimeError(f"Auditoria: campo {c} precisa ser inteiro e ≥ 0.")
    if not np.allclose(df_final["Valor_Compra_R$"].values, (df_final["Compra_Sugerida"] * df_final["Preco"]).round(2).values):
        raise RuntimeError("Auditoria: Valor_Compra_R$ inconsistente com Compra × Preco.")

    hash_str = sha256_of_csv(df_final)
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        lista = df_final[df_final["Compra_Sugerida"] > 0].copy()
        lista.to_excel(writer, sheet_name="Lista_Final", index=False)
        ws = writer.sheets["Lista_Final"]
        for i, col in enumerate(lista.columns):
            width = max(12, int(lista[col].astype(str).map(len).max()) + 2)
            ws.set_column(i, i, min(width, 40))
        ws.freeze_panes(1, 0); ws.autofilter(0, 0, len(lista), len(lista.columns)-1)

        if pendencias:
            pd.DataFrame(pendencias).to_excel(writer, sheet_name="Pendencias", index=False)

        ctrl = pd.DataFrame([{
            "data_hora": dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "h": h,
            "linhas_Lista_Final": int((df_final["Compra_Sugerida"] > 0).sum()),
            "soma_Compra_Sugerida": int(df_final["Compra_Sugerida"].sum()),
            "soma_Valor_Compra_R$": float(df_final["Valor_Compra_R$"].sum()),
            "hash_sha256": hash_str,
        } | params])
        ctrl.to_excel(writer, sheet_name="Controle", index=False)
    output.seek(0)
    return output.read()

# ===================== UI =====================
st.set_page_config(page_title="Reposição Logística — Alivvia", layout="wide")

# Estado por empresa
if "empresas" not in st.session_state:
    st.session_state.empresas = {"ALIVVIA": {"df_final": None, "painel": None},
                                 "JCA":     {"df_final": None, "painel": None}}

# ---------- Cabeçalho e seletor de empresa no topo ----------
c_title, c_emp = st.columns([0.7, 0.3])
with c_title:
    st.title("Reposição Logística — Alivvia")
    st.caption("FULL por anúncio; compra por componente; Shopee explode antes; painel de estoques; prévia por SKU; filtro por fornecedor.")

with c_emp:
    empresa = st.radio("Empresa ativa", ["ALIVVIA","JCA"], index=0, horizontal=True)
    emp_key = empresa

# ---------- Sidebar: Parâmetros + Padrão (via Secrets) ----------
with st.sidebar:
    st.subheader("Parâmetros")
    h  = st.selectbox("Horizonte (dias)", [30,60,90], index=1)
    g  = st.number_input("Crescimento % ao mês", value=0.0, step=1.0)
    LT = st.number_input("Lead time (dias)", value=0, step=1, min_value=0)
    st.markdown("**Arquivo local opcional:** `Padrao_produtos.xlsx` na mesma pasta.")

    # ====== Padrão (KITS/CAT) via Google Sheets (Secrets) ======
    def _build_export_url(edit_url: str) -> str:
        m = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", edit_url or "")
        return f"https://docs.google.com/spreadsheets/d/{m.group(1)}/export?format=xlsx" if m else ""

    PADRAO_EDIT_URL = st.secrets.get("PADRAO_EDIT_URL", "").strip()
    PADRAO_DL_URL   = st.secrets.get("PADRAO_DOWNLOAD_URL", "").strip() or _build_export_url(PADRAO_EDIT_URL)
    AUTO_BAIXAR     = str(st.secrets.get("AUTO_BAIXAR_PADRAO", "true")).lower() == "true"

    st.markdown("### Padrão (KITS/CAT) por link")
    if PADRAO_EDIT_URL:
        st.markdown(f"[🔗 Abrir no Drive (editar)]({PADRAO_EDIT_URL})")
    else:
        st.info("Configure PADRAO_EDIT_URL em *Secrets* para editar no Drive.")

    st.toggle("Baixar padrão automaticamente ao abrir", value=AUTO_BAIXAR, key="auto_baixar_padrao")

    if st.button("⬇️ Baixar padrão", use_container_width=True):
        if not PADRAO_DL_URL:
            st.error("PADRAO_DOWNLOAD_URL não configurado e não foi possível derivar do PADRAO_EDIT_URL.")
        else:
            try:
                resp = requests.get(PADRAO_DL_URL, timeout=30)
                resp.raise_for_status()
                xls_bytes = io.BytesIO(resp.content)
                st.session_state["catalogo"] = carregar_padrao_produtos(xls_bytes)
                st.success("Padrão baixado e carregado com sucesso.")
            except Exception as e:
                st.error(f"Falha ao baixar o padrão: {e}")

# Baixar automaticamente ao abrir (se habilitado e ainda não carregou)
if st.session_state.get("auto_baixar_padrao", False) and ("catalogo" not in st.session_state):
    PADRAO_EDIT_URL = st.secrets.get("PADRAO_EDIT_URL", "").strip()
    PADRAO_DL_URL   = st.secrets.get("PADRAO_DOWNLOAD_URL", "").strip() or (lambda u: f"https://docs.google.com/spreadsheets/d/{re.search(r'/spreadsheets/d/([a-zA-Z0-9-_]+)', u).group(1)}/export?format=xlsx" if re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", u) else "")(PADRAO_EDIT_URL)
    if PADRAO_DL_URL:
        try:
            resp = requests.get(PADRAO_DL_URL, timeout=30)
            resp.raise_for_status()
            xls_bytes = io.BytesIO(resp.content)
            st.session_state["catalogo"] = carregar_padrao_produtos(xls_bytes)
            st.toast("Padrão carregado automaticamente do Google Sheets.", icon="✅")
        except Exception as e:
            st.warning(f"Não consegui baixar o padrão automaticamente: {e}")

st.divider()

# ---------- Uploads por EMPRESA ----------
st.markdown(f"### Uploads — **{empresa}**")
c1, c2, c3 = st.columns(3)
with c1:
    st.markdown(f"**FULL (Magiic) — {empresa}**")
    full_file = st.file_uploader(" ", key=f"full_{emp_key}")
with c2:
    st.markdown(f"**Estoque Físico (CSV/XLSX/XLS) — {empresa}**")
    fisico_file = st.file_uploader("  ", key=f"fisico_{emp_key}")
with c3:
    st.markdown(f"**Shopee / Mercado Turbo (vendas por SKU) — {empresa}**")
    shopee_file = st.file_uploader("   ", key=f"vendas_{emp_key}")

st.divider()

# ---------- Gerar Compra por EMPRESA ----------
if st.button(f"Gerar Compra — {empresa}", type="primary"):
    try:
        # 1) Catálogo (prioriza Sheets baixado; senão tenta arquivo local)
        cat = st.session_state.get("catalogo")
        if cat is None:
            base_dir = os.path.dirname(os.path.abspath(__file__))
            cat_path = os.path.join(base_dir, "Padrao_produtos.xlsx")
            if os.path.exists(cat_path):
                cat = carregar_padrao_produtos(cat_path)
            else:
                raise RuntimeError("Padrão não carregado. Use os botões da sidebar (Drive) ou deixe Padrao_produtos.xlsx local.")

        # 2) Entradas
        dfs = []
        for up in [full_file, fisico_file, shopee_file]:
            if up is None: continue
            df_raw = load_any_table(up)
            tipo = mapear_tipo(df_raw)
            if tipo == "DESCONHECIDO":
                st.error(f"Arquivo '{up.name}' não reconhecido. Reexporte com colunas corretas.")
                st.stop()
            dfs.append((tipo, mapear_colunas(df_raw, tipo)))

        tipos_presentes = set(t for t,_ in dfs)
        faltantes = {"FULL","FISICO","VENDAS"} - tipos_presentes
        if faltantes:
            st.error(f"Entradas inválidas. Faltou: {', '.join(sorted(faltantes))}. Detectei: {', '.join(sorted(tipos_presentes))}.")
            st.stop()

        full_df   = [df for t, df in dfs if t == "FULL"][0]
        fisico_df = [df for t, df in dfs if t == "FISICO"][0]
        vendas_df = [df for t, df in dfs if t == "VENDAS"][0]

        df_final, painel = calcular(full_df, fisico_df, vendas_df, cat, h=h, g=g, LT=LT)

        st.session_state.empresas[emp_key]["df_final"] = df_final
        st.session_state.empresas[emp_key]["painel"]   = painel
        st.success(f"Cálculo concluído para **{empresa}**. Use os filtros abaixo sem recálculo.")
    except Exception as e:
        st.error(str(e))
        st.stop()

# ---------- Render pós-cálculo ----------
dados_emp = st.session_state.empresas[emp_key]
if dados_emp["df_final"] is not None:
    df_final = dados_emp["df_final"].copy()
    painel   = dados_emp["painel"]

    st.subheader(f"📊 Painel de Estoques — {empresa}")
    cA, cB, cC, cD = st.columns(4)
    cA.metric("Full (un)",  f"{painel['full_unid']:,}".replace(",", "."))
    cB.metric("Full (R$)",  f"R$ {painel['full_valor']:,.2f}")
    cC.metric("Físico (un)",f"{painel['fisico_unid']:,}".replace(",", "."))
    cD.metric("Físico (R$)",f"R$ {painel['fisico_valor']:,.2f}")

    st.divider()

    fornecedores = sorted(df_final["fornecedor"].fillna("").unique())
    sel_fornec = st.multiselect("Filtrar por Fornecedor", fornecedores, key=f"filt_fornec_{emp_key}")
    mostra = df_final if not sel_fornec else df_final[df_final["fornecedor"].isin(sel_fornec)]

    with st.expander(f"🔎 Prévia por SKU — {empresa}"):
        sku_opts = sorted(mostra["SKU"].unique())
        sel_skus = st.multiselect("Escolha 1 ou mais SKUs", sku_opts, key=f"filt_sku_preview_{emp_key}")
        prev = mostra if not sel_skus else mostra[mostra["SKU"].isin(sel_skus)]
        st.dataframe(
            prev[[
                "SKU","fornecedor","ML_60d","Shopee_60d","TOTAL_60d",
                "Estoque_Fisico","Reserva_30d","Folga_Fisico",
                "Necessidade","Compra_Sugerida","Preco"
            ]],
            use_container_width=True,
            height=380
        )
        st.caption("Compra = Necessidade − Folga (nunca negativa). Vendas 60d já explodidas e Shopee normalizada.")

    st.subheader(f"Itens para comprar — {empresa}")
    st.dataframe(
        mostra[mostra["Compra_Sugerida"] > 0][[
            "SKU","fornecedor","Vendas_h_ML","Vendas_h_Shopee",
            "Estoque_Fisico","Preco","Compra_Sugerida","Valor_Compra_R$"
        ]],
        use_container_width=True
    )

    compra_total = int(mostra["Compra_Sugerida"].sum())
    valor_total  = float(mostra["Valor_Compra_R$"].sum())
    st.success(f"{len(mostra[mostra['Compra_Sugerida']>0])} SKUs com compra > 0 | Compra total: {compra_total} un | Valor: R$ {valor_total:,.2f}")

    st.subheader("Exportação XLSX (Lista_Final + Controle)")
    if st.checkbox("Gerar planilha XLSX com hash e sanity (sem recálculo)?", key=f"chk_export_{emp_key}"):
        try:
            xlsx_bytes = exportar_xlsx(mostra, h=h, params={"g": g, "LT": LT, "empresa": empresa})
            st.download_button(
                label=f"Baixar XLSX — Compra_Sugerida_{h}d_{empresa}.xlsx",
                data=xlsx_bytes,
                file_name=f"Compra_Sugerida_{h}d_{empresa}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key=f"btn_dl_{emp_key}"
            )
            st.info("Planilha gerada a partir do mesmo DataFrame exibido (paridade garantida).")
        except Exception as e:
            st.error(f"Exportação bloqueada pela Auditoria: {e}")

st.caption("© Alivvia — simples, robusto e auditável.")
