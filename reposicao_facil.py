# Reposição Logística — Alivvia (Streamlit) | multi-empresa + padrão por link (Drive)
# Entradas: FULL (Magiic) + ESTOQUE FÍSICO + SHOPEE/MT (vendas). Compra por componente.
# Resultados ficam em session_state por EMPRESA (sem recálculo ao filtrar).

import os
import io
import hashlib
import datetime as dt
from dataclasses import dataclass

import numpy as np
import pandas as pd
from unidecode import unidecode
import streamlit as st
import requests  # NOVO: para baixar planilha do Drive/URL

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
        raise RuntimeError(f"Não consegui abrir '{getattr(caminho_ou_buffer, 'name', caminho_ou_buffer)}'. "
                           f"Deixe na mesma pasta do app. Erro: {e}")

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
    df_kits = df_kits[df_kits["qty"] >= 1]
    df_kits = df_kits.drop_duplicates(subset=["kit_sku", "component_sku"], keep="first")

    df_cat.columns = [norm_header(c) for c in df_cat.columns]
    if "component_sku" not in df_cat.columns:
        raise RuntimeError("CATALOGO_SIMPLES precisa ter a coluna 'component_sku'.")
    if "fornecedor" not in df_cat.columns:
        df_cat["fornecedor"] = ""
    if "status_reposicao" not in df_cat.columns:
        df_cat["status_reposicao"] = ""

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
    kits = kits.drop_duplicates(subset=["kit_sku", "component_sku"], keep="first")
    return kits

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
        raise RuntimeError(f"Não consegui ler o arquivo '{uploaded_file.name}': {e}")

    df.columns = [norm_header(c) for c in df.columns]

    # FULL com header na 3ª linha (caso tenha 2 linhas de título)
    if ("sku" not in df.columns) and ("codigo" not in df.columns) and ("codigo_sku" not in df.columns) and len(df) > 0:
        try:
            uploaded_file.seek(0)
            df = pd.read_excel(uploaded_file, dtype=str, keep_default_na=False, header=2)
            df.columns = [norm_header(c) for c in df.columns]
        except Exception:
            pass

    # limpar TOTAL/TOTAIS e SKU vazio
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

    # FULL
    tem_vendas60 = any(c.startswith("vendas_60d") or c in {"vendas 60d","vendas_qtd_60d"} for c in cols)
    tem_estoque  = any(c in {"estoque_full","estoque_atual"} for c in cols)
    tem_transito = any(c in {"em_transito","em transito","em_transito_full","em_transito_do_anuncio"} for c in cols)
    tem_sku_std  = any(c in {"sku","codigo","codigo_sku"} for c in cols)
    if tem_sku_std and ((tem_vendas60 and tem_estoque) or (tem_vendas60 and tem_transito) or (tem_estoque and tem_transito)):
        return "FULL"

    # FÍSICO
    tem_preco = any(c in {"preco","preco_compra","preco_medio","custo","custo_medio"} for c in cols)
    tem_estoque_fis = any(c in {"estoque_atual","qtd","quantidade"} for c in cols)
    tem_sku_livre = any("sku" in c for c in cols)
    if tem_sku_livre and tem_estoque_fis and tem_preco:
        return "FISICO"

    # VENDAS
    tem_qtd_livre = any(("qtde" in c) or ("quant" in c) or ("venda" in c) or ("order" in c) for c in cols)
    if tem_sku_livre and tem_qtd_livre and not tem_preco:
        return "VENDAS"

    return "DESCONHECIDO"

# ===================== Mapeamento de colunas =====================
def mapear_colunas(df: pd.DataFrame, tipo: str) -> pd.DataFrame:
    if tipo == "FULL":
        if "sku" in df.columns:
            df["SKU"] = df["sku"].map(norm_sku)
        elif "codigo" in df.columns:
            df["SKU"] = df["codigo"].map(norm_sku)
        elif "codigo_sku" in df.columns:
            df["SKU"] = df["codigo_sku"].map(norm_sku)
        else:
            raise RuntimeError("FULL inválido: precisa de coluna SKU/codigo.")

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
        sku_series = (
            df["sku"] if "sku" in df.columns else
            (df["codigo"] if "codigo" in df.columns else
             (df["codigo_sku"] if "codigo_sku" in df.columns else None))
        )
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
        if sku_col is None:
            raise RuntimeError("VENDAS inválido: não achei coluna de SKU.")
        df["SKU"] = df[sku_col].map(norm_sku)

        cand_qty = []
        for c in df.columns:
            cl = c.lower()
            score = 0
            if "qtde"  in cl: score += 3
            if "quant" in cl: score += 2
            if "venda" in cl: score += 1
            if "order" in cl: score += 1
            if score > 0:
                cand_qty.append((score, c))
        if not cand_qty:
            raise RuntimeError("VENDAS inválido: não achei coluna de Quantidade.")
        cand_qty.sort(reverse=True)
        qcol = cand_qty[0][1]

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
    out = out.rename(columns={"component_sku":"SKU","quantidade_comp":"Quantidade"})
    return out

def calcular(full_df, fisico_df, vendas_df, cat: Catalogo, h=60, g=0.0, LT=0):
    kits = construir_kits_efetivo(cat)

    # FULL (por anúncio)
    full = full_df.copy()
    full["SKU"]             = full["SKU"].map(norm_sku)
    full["Vendas_Qtd_60d"]  = full["Vendas_Qtd_60d"].astype(int)
    full["Estoque_Full"]    = full["Estoque_Full"].astype(int)
    full["Em_Transito"]     = full["Em_Transito"].astype(int)

    # Shopee/MT
    shp = vendas_df.copy()
    shp["SKU"]              = shp["SKU"].map(norm_sku)
    shp["Quantidade_60d"]   = shp["Quantidade"].astype(int)

    # Explodir p/ componentes
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

    # Físico
    fis = fisico_df.copy()
    fis["SKU"]            = fis["SKU"].map(norm_sku)
    fis["Estoque_Fisico"] = fis["Estoque_Fisico"].fillna(0).astype(int)
    fis["Preco"]          = fis["Preco"].fillna(0.0)

    base = demanda.merge(fis, on="SKU", how="left")
    base["Estoque_Fisico"] = base["Estoque_Fisico"].fillna(0).astype(int)
    base["Preco"]          = base["Preco"].fillna(0.0)

    # Planejamento por anúncio → envio desejado
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

    # Reserva 30d
    base["Demanda_dia"]  = base["TOTAL_60d"] / 60.0
    base["Reserva_30d"]  = np.round(base["Demanda_dia"] * 30).astype(int)
    base["Folga_Fisico"] = (base["Estoque_Fisico"] - base["Reserva_30d"]).clip(lower=0).astype(int)

    # Compra
    base["Compra_Sugerida"] = (base["Necessidade"] - base["Folga_Fisico"]).clip(lower=0).astype(int)

    # status_reposicao = nao_repor ⇒ zera compra
    mask_nao = base["status_reposicao"].str.lower().str.contains("nao_repor", na=False)
    base.loc[mask_nao, "Compra_Sugerida"] = 0

    # Valores
    base["Valor_Compra_R$"] = (base["Compra_Sugerida"].astype(float) * base["Preco"].astype(float)).round(2)

    # Vendas no horizonte h
    base["Vendas_h_ML"]     = np.round(base["ML_60d"] * (h/60.0)).astype(int)
    base["Vendas_h_Shopee"] = np.round(base["Shopee_60d"] * (h/60.0)).astype(int)

    base = base.sort_values(["fornecedor","Valor_Compra_R$","SKU"], ascending=[True, False, True])

    df_final = base[[
        "SKU","fornecedor",
        "Vendas_h_ML","Vendas_h_Shopee",
        "Estoque_Fisico","Preco","Compra_Sugerida","Valor_Compra_R$",
        "ML_60d","Shopee_60d","TOTAL_60d","Reserva_30d","Folga_Fisico","Necessidade"
    ]].reset_index(drop=True)

    # Painel de estoques (Full e Físico)
    fis_unid  = int(fis["Estoque_Fisico"].sum())
    fis_valor = float((fis["Estoque_Fisico"] * fis["Preco"]).sum())

    full_stock_comp = explodir_por_kits(
        full[["SKU","Estoque_Full"]].rename(columns={"SKU":"kit_sku","Estoque_Full":"Qtd"}),
        kits,"kit_sku","Qtd"
    )
    full_stock_comp = full_stock_comp.merge(fis[["SKU","Preco"]], on="SKU", how="left")
    full_unid  = int(full["Estoque_Full"].sum())
    full_valor = float((full_stock_comp["Quantidade"].fillna(0) * full_stock_comp["Preco"].fillna(0.0)).sum())

    painel = {"full_unid": full_unid, "full_valor": full_valor, "fisico_unid": fis_unid, "fisico_valor": fis_valor}
    return df_final, painel

# ===================== Export XLSX =====================
def sha256_of_csv(df: pd.DataFrame) -> str:
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    return hashlib.sha256(csv_bytes).hexdigest()

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

# ===================== NOVO: Padrão por URL (Drive) =====================
PADRAO_URL_EDIT_DEFAULT = ""  # cole seu link de edição (opcional)
PADRAO_URL_RAW_DEFAULT  = ""  # cole seu link de export (opcional)

def carregar_padrao_por_url(url: str) -> Catalogo:
    if not url:
        raise RuntimeError("URL do padrão não informada.")
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        raise RuntimeError(f"Falha ao baixar o padrão: {e}")
    data = io.BytesIO(resp.content)
    return carregar_padrao_produtos(data)

# ===================== UI =====================
st.set_page_config(page_title="Reposição Logística — Alivvia", layout="wide")

# estado inicial por empresa
if "empresas" not in st.session_state:
    st.session_state.empresas = {
        "ALIVVIA": {"df_final": None, "painel": None},
        "JCA":     {"df_final": None, "painel": None},
    }

# ---- Cabeçalho e seletor de empresa (topo)
c_title, c_emp = st.columns([0.7, 0.3])
with c_title:
    st.title("Reposição Logística — Alivvia")
    st.caption("FULL por anúncio; compra por componente; Shopee explode antes; painel de estoques; "
               "prévia por SKU; filtro por fornecedor: resultados por EMPRESA, sem recálculo ao filtrar.")

with c_emp:
    empresa = st.radio("Empresa ativa", ["ALIVVIA", "JCA"], index=0, horizontal=True)
    emp_key = empresa  # "ALIVVIA" ou "JCA"

# Sidebar — parâmetros e Padrão por link (Drive)
with st.sidebar:
    st.subheader("Parâmetros")
    h  = st.selectbox("Horizonte (dias)", [30, 60, 90], index=1)
    g  = st.number_input("Crescimento % ao mês", value=0.0, step=1.0)
    LT = st.number_input("Lead time (dias)", value=0, step=1, min_value=0)
    st.markdown("**Arquivo local (mesma pasta):** `Padrao_produtos.xlsx`")

    st.markdown("---")
    st.markdown("### Padrão (KITS/CAT) por link")

    padrao_url_edit = st.text_input(
        "URL de edição (Google Sheets / Drive)",
        value=st.session_state.get("padrao_url_edit", PADRAO_URL_EDIT_DEFAULT),
        help="Link normal do Google Sheets para abrir e editar."
    )
    st.session_state["padrao_url_edit"] = padrao_url_edit

    padrao_url_raw = st.text_input(
        "URL direta para baixar (opcional)",
        value=st.session_state.get("padrao_url_raw", PADRAO_URL_RAW_DEFAULT),
        help="Link de export (ex.: .../export?format=xlsx) para baixar automaticamente."
    )
    st.session_state["padrao_url_raw"] = padrao_url_raw

    c1, c2 = st.columns(2)
    with c1:
        if padrao_url_edit:
            st.link_button("🔗 Abrir no Drive", padrao_url_edit, help="Abrir planilha para editar.")
        else:
            st.caption("Cole o link de edição acima.")
    with c2:
        if st.button("⬇️ Baixar padrão", use_container_width=True, help="Validar download do padrão"):
            try:
                if not padrao_url_raw:
                    st.warning("Informe a URL direta de download para baixar automaticamente.")
                else:
                    _ = carregar_padrao_por_url(padrao_url_raw)
                    st.success("Padrão baixado com sucesso! (Será usado ao gerar a compra.)")
            except Exception as e:
                st.error(str(e))

st.divider()

# ---- Área de Uploads (por EMPRESA)
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

# ------- Botão: calcula e salva por EMPRESA (sem recálculo ao filtrar)
if st.button(f"Gerar Compra — {empresa}", type="primary"):
    try:
        # 1) Carregar padrão: prioriza URL direta (se houver), senão arquivo local
        cat = None
        url_raw = st.session_state.get("padrao_url_raw", "")
        if url_raw:
            try:
                cat = carregar_padrao_por_url(url_raw)
            except Exception as e:
                st.warning(f"Não consegui baixar do link; usando arquivo local. Detalhe: {e}")
        if cat is None:
            base_dir = os.path.dirname(os.path.abspath(__file__))
            cat_path = os.path.join(base_dir, "Padrao_produtos.xlsx")
            cat = carregar_padrao_produtos(cat_path)

        # 2) Ler e mapear as 3 entradas
        dfs, tipos = [], {}
        for up in [full_file, fisico_file, shopee_file]:
            if up is None:
                continue
            df_raw = load_any_table(up)
            t = mapear_tipo(df_raw)
            if t == "DESCONHECIDO":
                st.error(f"Arquivo '{up.name}' não reconhecido. Reexporte com colunas corretas.")
                st.stop()
            tipos[t] = up.name
            dfs.append((t, mapear_colunas(df_raw, t)))

        tipos_presentes = set([t for t, _ in dfs])
        faltantes = {"FULL", "FISICO", "VENDAS"} - tipos_presentes
        if faltantes:
            st.error(f"Entradas inválidas. Faltou: {', '.join(sorted(faltantes))}. Detectei: {', '.join(sorted(tipos_presentes))}.")
            st.stop()

        full_df   = [df for t, df in dfs if t == "FULL"][0]
        fisico_df = [df for t, df in dfs if t == "FISICO"][0]
        vendas_df = [df for t, df in dfs if t == "VENDAS"][0]

        df_final, painel = calcular(full_df, fisico_df, vendas_df, cat, h=h, g=g, LT=LT)

        # ► Salva o resultado no estado por EMPRESA
        st.session_state.empresas[emp_key]["df_final"] = df_final
        st.session_state.empresas[emp_key]["painel"]   = painel

        st.success(f"Cálculo concluído para **{empresa}**. Use os filtros abaixo sem recálculo.")
    except Exception as e:
        st.error(str(e))
        st.stop()

# ================= RENDER PÓS-CÁLCULO (por EMPRESA) =================
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

    # ===== Filtro por Fornecedor
    fornecedores = sorted(df_final["fornecedor"].fillna("").unique())
    sel_fornec = st.multiselect("Filtrar por Fornecedor", fornecedores, key=f"filt_fornec_{emp_key}")

    mostra = df_final if not sel_fornec else df_final[df_final["fornecedor"].isin(sel_fornec)]

    # ===== Prévia por SKU
    with st.expander(f"🔎 Prévia por SKU — {empresa} (opcional)"):
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
    st.success(f"{len(mostra[mostra['Compra_Sugerida']>0])} SKUs com compra > 0 | "
               f"Compra total: {compra_total} un | Valor: R$ {valor_total:,.2f}")

    st.subheader("Exportação XLSX (Lista_Final + Controle)")
    if st.checkbox("Gerar planilha XLSX com hash e sanity (sem recálculo)?", key=f"chk_export_{emp_key}"):
        try:
            xlsx_bytes = exportar_xlsx(mostra, h=h, params={"g": g, "LT": LT})
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
