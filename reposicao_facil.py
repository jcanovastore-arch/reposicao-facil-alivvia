# Reposição Logística — Alivvia (Streamlit)
# FULL por anúncio; compra por componente; Shopee explode antes; painel de estoques;
# prévia por SKU; filtro por fornecedor. Resultados por EMPRESA, sem recálculo ao filtrar.

import os
import io
import json
import hashlib
import datetime as dt
from dataclasses import dataclass

import numpy as np
import pandas as pd
import requests
from unidecode import unidecode
import streamlit as st

# ============================
# Configurações gerais
# ============================
st.set_page_config(page_title="Reposição Logística — Alivvia", layout="wide")

# Estado persistente (JSON local)
APP_STATE_FILE = "estado_app.json"       # salva automaticamente (por empresa)
EMPRESAS = ["ALIVVIA", "JCA"]            # duas empresas separadas
PADRAO_LOCAL_NOME = "Padrao_produtos.xlsx"

# Google Sheets - seu ID padrão (pode editar se trocar de planilha no futuro)
SHEET_ID_DEFAULT = "1cTLARjq-B5g50dL6tcntg7lb_Iu0ta43"
PADRAO_URL_EDIT_BASE   = "https://docs.google.com/spreadsheets/d/{}/edit"
PADRAO_URL_EXPORT_BASE = "https://docs.google.com/spreadsheets/d/{}/export?format=xlsx"


# ============================
# Helpers de estado (persistência)
# ============================
def _load_json(path: str) -> dict:
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def _save_json(path: str, data: dict):
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        st.warning(f"Não consegui salvar estado: {e}")

def get_state() -> dict:
    if "STATE" not in st.session_state:
        st.session_state.STATE = _load_json(APP_STATE_FILE) or {}
        # estrutura base
        for emp in EMPRESAS:
            st.session_state.STATE.setdefault(emp, {})
            st.session_state.STATE[emp].setdefault("compras_manuais", [])   # compras digitadas
            st.session_state.STATE[emp].setdefault("semanais", [])          # lista de SKUs semanais
        # global
        st.session_state.STATE.setdefault("gsheet_edit_link", PADRAO_URL_EDIT_BASE.format(SHEET_ID_DEFAULT))
        st.session_state.STATE.setdefault("auto_baixar_padrao", False)
    return st.session_state.STATE

def save_state():
    _save_json(APP_STATE_FILE, st.session_state.STATE)


STATE = get_state()


# ============================
# Google Sheets (editar/baixar padrão)
# ============================
def gsheet_to_export_xlsx_link(edit_or_share_link: str) -> str:
    """
    Converte link de edição/compartilhamento do Google Sheets em link de export XLSX.
    Ex.: https://docs.google.com/spreadsheets/d/{ID}/edit -> .../export?format=xlsx
    """
    if not edit_or_share_link:
        return ""
    try:
        if "/spreadsheets/d/" in edit_or_share_link:
            after = edit_or_share_link.split("/spreadsheets/d/")[1]
            sheet_id = after.split("/")[0]
            return PADRAO_URL_EXPORT_BASE.format(sheet_id)
    except Exception:
        pass
    return ""

def baixar_padrao_do_drive(url_export: str, destino: str) -> tuple[bool, str]:
    """
    Baixa o XLSX do Google Sheets (público) para `destino`.
    """
    try:
        if not url_export:
            return False, "Link de exportação vazio."
        r = requests.get(url_export, timeout=30)
        if r.status_code != 200:
            return False, f"HTTP {r.status_code} ao baixar."
        with open(destino, "wb") as f:
            f.write(r.content)
        if not os.path.exists(destino) or os.path.getsize(destino) < 1024:
            return False, "Arquivo baixado ficou muito pequeno / vazio."
        return True, f"Padrão salvo em: {destino}"
    except Exception as e:
        return False, f"Erro ao baixar: {e}"


# =============== Utils (originais) ===============
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
    # inclui "." para casos tipo "Qtde. Vendas" -> qtde_vendas
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


# =============== Padrão produtos (KITS + CAT) ===============
def carregar_padrao_produtos(caminho: str) -> Catalogo:
    try:
        xls = pd.ExcelFile(caminho)
    except Exception as e:
        raise RuntimeError(f"Não consegui abrir '{caminho}'. Baixe na sidebar. Erro: {e}")

    def load_sheet(opts):
        for n in opts:
            if n in xls.sheet_names:
                return pd.read_excel(xls, n)
        raise RuntimeError(f"Aba não encontrada. Esperado uma de {opts} no '{caminho}'.")

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

    df_cat["component_sku"]   = df_cat["component_sku"].map(norm_sku)
    df_cat["fornecedor"]      = df_cat["fornecedor"].fillna("").astype(str)
    df_cat["status_reposicao"]= df_cat["status_reposicao"].fillna("").astype(str)

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


# =============== Leitura genérica (robusta) ===============
def load_any_table(uploaded_file) -> pd.DataFrame:
    if uploaded_file is None:
        return None
    name = uploaded_file.name.lower()
    try:
        if name.endswith(".csv"):
            uploaded_file.seek(0)
            df = pd.read_csv(uploaded_file, dtype=str, keep_default_na=False, sep=None, engine="python")
        elif name.endswith(".xlsx"):
            uploaded_file.seek(0)
            df = pd.read_excel(uploaded_file, dtype=str, keep_default_na=False, engine="openpyxl")
        elif name.endswith(".xls"):
            uploaded_file.seek(0)
            df = pd.read_excel(uploaded_file, dtype=str, keep_default_na=False, engine="xlrd")
        else:
            raise RuntimeError("Formato não suportado. Use CSV/XLSX/XLS.")
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


# =============== Detecção por conteúdo (tolerante) ===============
def mapear_tipo(df: pd.DataFrame) -> str:
    cols = [c.lower() for c in df.columns]

    # FULL: tem vendas_60d + (estoque/transito) + SKU convencional
    tem_vendas60 = any(c.startswith("vendas_60d") or c in {"vendas 60d","vendas_qtd_60d"} for c in cols)
    tem_estoque  = any(c in {"estoque_full","estoque_atual"} for c in cols)
    tem_transito = any(c in {"em_transito","em transito","em_transito_full","em_transito_do_anuncio"} for c in cols)
    tem_sku_std  = any(c in {"sku","codigo","codigo_sku"} for c in cols)
    if tem_sku_std and ((tem_vendas60 and tem_estoque) or (tem_vendas60 and tem_transito) or (tem_estoque and tem_transito)):
        return "FULL"

    # FÍSICO: tem alguma SKU + estoque + preço
    tem_preco = any(c in {"preco","preco_compra","preco_medio","custo","custo_medio"} for c in cols)
    tem_estoque_fis = any(c in {"estoque_atual","qtd","quantidade"} for c in cols)
    tem_sku_livre = any("sku" in c for c in cols)
    if tem_sku_livre and tem_estoque_fis and tem_preco:
        return "FISICO"

    # VENDAS: tem alguma SKU + alguma quantidade (qtde/quant/venda/order) e não tem preço
    tem_qtd_livre = any(("qtde" in c) or ("quant" in c) or ("venda" in c) or ("order" in c) for c in cols)
    if tem_sku_livre and tem_qtd_livre and not tem_preco:
        return "VENDAS"

    return "DESCONHECIDO"


# =============== Mapeamento de colunas (tolerante) ===============
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
            # fallback: primeira coluna que contenha 'sku'
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
        # SKU = primeira coluna que contenha 'sku'
        sku_col = next((c for c in df.columns if "sku" in c.lower()), None)
        if sku_col is None:
            raise RuntimeError("VENDAS inválido: não achei coluna de SKU (ex.: SKU, Model SKU, Variation SKU).")
        df["SKU"] = df[sku_col].map(norm_sku)

        # Quantidade = melhor match por palavras (qtde/quant/venda/order)
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
            raise RuntimeError("VENDAS inválido: não achei coluna de Quantidade (ex.: Qtde. Vendas, Quantidade, Orders).")
        cand_qty.sort(reverse=True)
        qcol = cand_qty[0][1]

        df["Quantidade"] = df[qcol].map(br_to_float).fillna(0).astype(int)
        return df[["SKU","Quantidade"]].copy()

    raise RuntimeError("Tipo de arquivo desconhecido.")


# =============== Explosão e Cálculo (SEU CÁLCULO ORIGINAL) ===============
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

    # Shopee/MT → já 60d
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

    # Reserva Shopee 30d
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
    return df_final


# =============== Export XLSX (sem recálculo) ===============
def sha256_of_csv(df: pd.DataFrame) -> str:
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    return hashlib.sha256(csv_bytes).hexdigest()

def exportar_xlsx(df_final: pd.DataFrame, h: int, params: dict, pendencias: list | None = None) -> bytes:
    for c in ["Vendas_h_ML","Vendas_h_Shopee","Estoque_Fisico","Compra_Sugerida",
              "Reserva_30d","Folga_Fisico","Necessidade","ML_60d","Shopee_60d","TOTAL_60d"]:
        if (df_final[c] < 0).any() or (df_final[c].astype(float) % 1 != 0).any():
            raise RuntimeError(f"Auditoria: campo {c} precisa ser inteiro e ≥ 0.")
    if not np.allclose(df_final["Valor_Compra_R$"].values,
                       (df_final["Compra_Sugerida"] * df_final["Preco"]).round(2).values):
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


# ============================
# UI
# ============================
st.title("Reposição Logística — Alivvia")
st.caption("FULL por anúncio; compra por componente; Shopee explode antes; painel de estoques; prévia por SKU; filtro por fornecedor. Resultados por EMPRESA e sem recálculo ao filtrar.")

# ---- Empresa ativa (separação real de uploads/estado) ----
empresa_ativa = st.radio("Empresa ativa", EMPRESAS, horizontal=True, key="empresa_ativa_top")

# ---- Parâmetros ----
with st.sidebar:
    st.subheader("Parâmetros")
    h  = st.selectbox("Horizonte (dias)", [30, 60, 90], index=1)
    g  = st.number_input("Crescimento % ao mês", value=0.0, step=1.0)
    LT = st.number_input("Lead time (dias)", value=0, step=1, min_value=0)

    st.markdown("**Arquivo local opcional (será usado no cálculo):**")
    st.code(PADRAO_LOCAL_NOME, language=None)
    st.caption("Se existir na mesma pasta, será usado para cálculo.")

    st.markdown("---")
    st.subheader("Padrão (KITS/CAT) por link")
    # campo para armazenar o link de edição do Google (persistente)
    link_edit_atual = st.text_input(
        "URL de edição do Google (editar/abrir)",
        value=STATE.get("gsheet_edit_link", "")
    )
    if link_edit_atual != STATE.get("gsheet_edit_link"):
        STATE["gsheet_edit_link"] = link_edit_atual
        save_state()

    if STATE.get("gsheet_edit_link"):
        st.markdown(
            f"[🔗 Abrir no Google Sheets (editar)]({STATE['gsheet_edit_link']})",
            help="Abre a planilha para editar KITS e CATALOGO_SIMPLES."
        )

    # Campo de link manual (backup) — sempre visível
    link_export_manual = st.text_input(
        "URL direta para baixar (opcional)",
        value=gsheet_to_export_xlsx_link(STATE.get("gsheet_edit_link","")),
        help="Se precisar, cole aqui um link '.../export?format=xlsx'. O botão abaixo usará este link."
    )

    # Auto baixar
    auto_flag = st.checkbox(
        "Baixar padrão automaticamente ao abrir",
        value=STATE.get("auto_baixar_padrao", False),
        help="Se ligado, ao abrir o app ele baixa do Google Sheets e salva como 'Padrao_produtos.xlsx'."
    )
    if auto_flag != STATE.get("auto_baixar_padrao"):
        STATE["auto_baixar_padrao"] = auto_flag
        save_state()

    # Botão baixar
    if st.button("⬇ Baixar padrão", use_container_width=True):
        url_export = link_export_manual.strip() or gsheet_to_export_xlsx_link(STATE.get("gsheet_edit_link",""))
        ok, msg = baixar_padrao_do_drive(url_export, PADRAO_LOCAL_NOME)
        st.success(msg) if ok else st.warning(msg)

# Auto baixar ao abrir (uma vez por sessão)
if STATE.get("auto_baixar_padrao") and not st.session_state.get("baixou_padrao_auto"):
    url_export = gsheet_to_export_xlsx_link(STATE.get("gsheet_edit_link",""))
    ok, _msg = baixar_padrao_do_drive(url_export, PADRAO_LOCAL_NOME)
    st.session_state["baixou_padrao_auto"] = True

st.divider()

# ---- Uploads por empresa (separados) ----
st.subheader(f"Uploads — {empresa_ativa}")
col1, col2, col3 = st.columns(3)
with col1: full_file   = st.file_uploader(f"FULL (Magiic) — {empresa_ativa}", type=["csv","xlsx","xls"], key=f"full_{empresa_ativa}")
with col2: fisico_file = st.file_uploader(f"Estoque Físico — {empresa_ativa}", type=["csv","xlsx","xls"], key=f"fisico_{empresa_ativa}")
with col3: shopee_file = st.file_uploader(f"Shopee / Mercado Turbo (vendas por SKU) — {empresa_ativa}", type=["csv","xlsx","xls"], key=f"vendas_{empresa_ativa}")

st.divider()

# ------- Botão: calcular por EMPRESA e salvar no estado -------
if st.button(f"Gerar Compra — {empresa_ativa}", type="primary"):
    try:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        cat_path = os.path.join(base_dir, PADRAO_LOCAL_NOME)
        cat = carregar_padrao_produtos(cat_path)

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

        # ► Salva o resultado no estado da EMPRESA (sem recálculo ao filtrar)
        st.session_state[f"{empresa_ativa}_df_final"] = df_final
        st.session_state[f"{empresa_ativa}_painel"]   = painel

        st.success(f"Cálculo concluído para {empresa_ativa}. Use os filtros abaixo sem recálculo.")
    except Exception as e:
        st.error(str(e))
        st.stop()

# ================= RENDERIZAÇÃO PÓS-CÁLCULO (por EMPRESA, sem recálculo) ================
df_key = f"{empresa_ativa}_df_final"
painel_key = f"{empresa_ativa}_painel"

if st.session_state.get(df_key) is not None:
    df_final = st.session_state[df_key].copy()
    painel   = st.session_state.get(painel_key, {})

    # Painel
    st.subheader("📊 Painel de Estoques")
    cA, cB, cC, cD = st.columns(4)
    cA.metric("Full (un)",  f"{painel.get('full_unid',0):,}".replace(",", "."))
    cB.metric("Full (R$)",  f"R$ {painel.get('full_valor',0.0):,.2f}")
    cC.metric("Físico (un)",f"{painel.get('fisico_unid',0):,}".replace(",", "."))
    cD.metric("Físico (R$)",f"R$ {painel.get('fisico_valor',0.0):,.2f}")

    st.divider()

    # ===== Filtro por Fornecedor (sidebar, por empresa)
    with st.sidebar:
        st.subheader(f"Filtro por fornecedor — {empresa_ativa}")
        fornecedores = sorted(df_final["fornecedor"].fillna("").unique())
        sel_fornec = st.multiselect(
            "Escolha fornecedores",
            fornecedores,
            key=f"filt_fornec_{empresa_ativa}"
        )

    mostra = df_final if not sel_fornec else df_final[df_final["fornecedor"].isin(sel_fornec)]

    # ===== Semanais (marcar no app por empresa)
    with st.expander(f"⚙️ {empresa_ativa}: Gerenciar SKUs semanais"):
        sku_opts = sorted(mostra["SKU"].astype(str).unique())
        sem_atual = STATE[empresa_ativa].get("semanais", [])
        sem_select = st.multiselect("Marcar como semanais", sku_opts, default=sem_atual, key=f"sem_{empresa_ativa}")
        if sem_select != sem_atual:
            STATE[empresa_ativa]["semanais"] = sem_select
            save_state()
        st.caption("Só organiza/filtra; não altera o cálculo.")

    mostrar_semanais = st.checkbox("Mostrar apenas SKUs semanais", value=False, key=f"only_sem_{empresa_ativa}")
    if mostrar_semanais:
        sem_set = set(STATE[empresa_ativa].get("semanais", []))
        mostra = mostra[mostra["SKU"].astype(str).isin(sem_set)]

    # ===== Prévia por SKU com lista (evita erro de digitação)
    with st.expander("🔎 Prévia por SKU (opcional)"):
        sku_opts = sorted(mostra["SKU"].unique())
        sel_skus = st.multiselect("Escolha 1 ou mais SKUs", sku_opts, key=f"filt_sku_preview_{empresa_ativa}")
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

    # ===== Plano do mês + compras manuais + saldo (por empresa)
    st.subheader("Plano do mês + compras manuais + saldo")
    with st.form(f"compras_{empresa_ativa}"):
        colx, coly, colz = st.columns([2, 1, 1])
        with colx:
            sku_in = st.text_input("SKU", value="", key=f"cmp_sku_{empresa_ativa}")
        with coly:
            qtd_in = st.number_input("Qtd comprada", min_value=0, value=0, step=1, key=f"cmp_qtd_{empresa_ativa}")
        with colz:
            data_in = st.date_input("Data", value=dt.date.today(), key=f"cmp_data_{empresa_ativa}")
        add = st.form_submit_button("Adicionar compra do dia")
        if add and sku_in and qtd_in > 0:
            nova = {"data": str(data_in), "SKU": str(sku_in).upper().strip(), "qtd": int(qtd_in)}
            STATE[empresa_ativa]["compras_manuais"].append(nova)
            save_state()
            st.success(f"Compra adicionada: {nova}")

    compras_df = pd.DataFrame(STATE[empresa_ativa].get("compras_manuais", []))
    mostra_adj = mostra.copy()
    mostra_adj["qtd_comprada"] = 0
    if not compras_df.empty:
        agg = compras_df.groupby("SKU", as_index=False)["qtd"].sum().rename(columns={"qtd": "qtd_comprada"})
        mostra_adj = mostra_adj.merge(agg, on="SKU", how="left")
        mostra_adj["qtd_comprada"] = mostra_adj["qtd_comprada"].fillna(0).astype(int)
        mostra_adj["Compra_Sugerida"] = (mostra_adj["Compra_Sugerida"] - mostra_adj["qtd_comprada"]).clip(lower=0).astype(int)

    mostra_adj["Valor_Compra_R$"] = (mostra_adj["Compra_Sugerida"] * mostra_adj["Preco"]).astype(float)

    # ===== Itens para comprar (após filtros e ajustes)
    st.subheader(f"Itens para comprar — {empresa_ativa}")
    tver = mostra_adj[mostra_adj["Compra_Sugerida"] > 0].copy()
    st.dataframe(
        tver[[
            "SKU","fornecedor","Vendas_h_ML","Vendas_h_Shopee",
            "Estoque_Fisico","Preco","qtd_comprada","Compra_Sugerida","Valor_Compra_R$"
        ]],
        use_container_width=True,
        height=420
    )

    compra_total = int(mostra_adj["Compra_Sugerida"].sum())
    valor_total  = float(mostra_adj["Valor_Compra_R$"].sum())
    st.success(f"{len(tver)} SKUs com compra > 0 | Compra total: {compra_total} un | Valor: R$ {valor_total:,.2f}")

    # Resumo por fornecedor (após filtros/ajustes)
    with st.expander("📦 Resumo por fornecedor (após filtros/ajustes)"):
        if not tver.empty:
            grp = (tver.groupby("fornecedor", dropna=False, as_index=False)
                      .agg(SKUs=("SKU","nunique"),
                           Qtd_Total=("Compra_Sugerida","sum"),
                           Valor_Total=("Valor_Compra_R$","sum"))
                      .sort_values(["Valor_Total","Qtd_Total"], ascending=[False, False]))
            st.dataframe(grp, use_container_width=True)
        else:
            st.caption("Sem itens para comprar.")

    # ===== Exportação XLSX (Lista_Final + Controle) — usa o DataFrame filtrado/ajustado
    st.subheader("Exportação XLSX (Lista_Final + Controle)")
    if st.checkbox("Gerar planilha XLSX com hash e sanity (sem recálculo)?", key=f"chk_export_{empresa_ativa}"):
        try:
            xlsx_bytes = exportar_xlsx(mostra_adj, h=h, params={"g": g, "LT": LT})
            st.download_button(
                label=f"Baixar XLSX — Compra_{empresa_ativa}_{h}d.xlsx",
                data=xlsx_bytes,
                file_name=f"Compra_{empresa_ativa}_{h}d.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key=f"btn_dl_{empresa_ativa}"
            )
            st.info("Planilha gerada a partir do mesmo DataFrame exibido (paridade garantida).")
        except Exception as e:
            st.error(f"Exportação bloqueada pela Auditoria: {e}")

    # Compras manuais já registradas (visual)
    with st.expander(f"🧾 Compras manuais registradas — {empresa_ativa}"):
        if not compras_df.empty:
            st.dataframe(compras_df, use_container_width=True)
        else:
            st.caption("Sem compras manuais registradas ainda.")

# ===== SALVAR/BAIXAR ESTADO (backup manual) =====
st.markdown("---")
col_a, col_b = st.columns([1,1])
with col_a:
    if st.button("💾 Salvar estado agora (forçado)"):
        save_state()
        st.success("Estado salvo.")

with col_b:
    jb = json.dumps(STATE, ensure_ascii=False, indent=2).encode("utf-8")
    st.download_button("⬇️ Baixar JSON de estado", data=jb, file_name="estado_app_backup.json", mime="application/json")

# Auto save no final
save_state()

st.caption("© Alivvia — simples, robusto e auditável.")
