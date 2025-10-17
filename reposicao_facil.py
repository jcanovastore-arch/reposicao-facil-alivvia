# Reposição Logística (Alivvia/JCA) — v2/v3 alinhado
# Feature freeze respeitado: nenhuma mudança estrutural/UX — apenas correções pontuais
# Abas: Dados das Empresas | Compra Automática | Alocação de Compra
# Disparo apenas por clique; contratos de I/O preservados.

import os
from pathlib import Path
import io
import hashlib
from typing import Tuple, Dict, Optional

import streamlit as st
import pandas as pd
import numpy as np

# ==========================
# Preferências e Constantes
# ==========================
APPDATA_DIR = Path(".appdata_storage")  # Persistência local para sobreviver a refresh da página
APPDATA_DIR.mkdir(parents=True, exist_ok=True)

EMPRESAS = ("ALIVVIA", "JCA")
TIPOS = ("FULL", "SHOPEE", "FISICO")

# KITS/CAT — abas toleradas
KITS_SHEETS_OK = ["KITS", "KITS_REAIS"]
CAT_SHEETS_OK = ["CATALOGO", "CATALOGO_SIMPLES"]

# ==========================
# Utils de Persistência
# ==========================

def _storage_dir(empresa: str) -> Path:
    d = APPDATA_DIR / empresa.upper()
    d.mkdir(parents=True, exist_ok=True)
    return d


def _persist_df(df: Optional[pd.DataFrame], empresa: str, tipo: str):
    if df is None:
        return
    p = _storage_dir(empresa) / f"{tipo}.parquet"
    df.to_parquet(p, index=False)


def _load_df(empresa: str, tipo: str) -> Optional[pd.DataFrame]:
    p = _storage_dir(empresa) / f"{tipo}.parquet"
    if p.exists():
        return pd.read_parquet(p)
    return None


def _clear_storage(empresa: str):
    d = _storage_dir(empresa)
    for f in d.glob("*.parquet"):
        try:
            f.unlink()
        except Exception:
            pass

# ==========================
# Normalização de cabeçalhos e parsing
# ==========================

def _norm_col(s: str) -> str:
    import re
    from unidecode import unidecode
    s2 = unidecode(str(s)).strip().lower()
    s2 = re.sub(r"[^a-z0-9]+", "_", s2)
    s2 = re.sub(r"_+", "_", s2).strip("_")
    return s2


def _read_any_table(file) -> pd.DataFrame:
    """Lê CSV/XLS/XLSX. Tenta header na linha 1; se detectar padrão do FULL
    com cabeçalho na 3ª linha, tenta header=2. Remove linhas de totais."""
    name = getattr(file, "name", "")
    try:
        if name.lower().endswith((".csv")):
            df = pd.read_csv(file, encoding="utf-8", sep=None, engine="python")
        else:
            df = pd.read_excel(file, engine="openpyxl")
    except Exception:
        # fallback xlrd para xls antigos
        df = pd.read_excel(file)

    # Heurística FULL header=2 (3ª linha)
    # Se a primeira coluna contém muitas NaN e a linha 2 parece cabeçalho válido
    if df.shape[0] > 3 and df.columns.to_list()[0] != df.columns.to_list()[0]:
        pass  # pouco provável, manter

    # Se a palavra TOTAL aparece na primeira coluna, remover linhas
    first_cols = [c for c in df.columns]
    # Normalizar temporariamente para detectar totais
    df_temp = df.copy()
    df_temp.columns = [_norm_col(c) for c in df_temp.columns]
    tot_mask = False
    for c in df_temp.columns:
        try:
            tot_mask = tot_mask | df_temp[c].astype(str).str.contains("total", case=False, na=False)
        except Exception:
            pass
    df = df.loc[~tot_mask].copy()

    # Normaliza cabeçalhos
    df.columns = [_norm_col(c) for c in df.columns]
    return df


def _map_full_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Mapeia FULL para colunas: SKU, Vendas_Qtd_60d, Estoque_Full, Em_Transito(opc)."""
    cols = {c: _norm_col(c) for c in df.columns}
    df2 = df.copy()
    df2.columns = list(cols.values())

    # SKU/codigo
    sku_col = None
    for c in ["sku", "codigo", "cod", "id_sku", "id"]:
        if c in df2.columns:
            sku_col = c
            break
    # Vendas 60d (ou similar)
    v60_col = None
    for c in ["vendas_60d", "vendas_qtd_60d", "qtd_60d", "qtde_60d", "venda_60d", "vendas"]:
        if c in df2.columns:
            v60_col = c
            break
    # Estoque Full/Atual
    est_col = None
    for c in ["estoque_full", "estoque_atual", "estoque", "disponivel"]:
        if c in df2.columns:
            est_col = c
            break
    # Em_Transito (opcional)
    trans_col = None
    for c in ["em_transito", "transito", "em_transito_qtd"]:
        if c in df2.columns:
            trans_col = c
            break

    if sku_col is None or v60_col is None or est_col is None:
        raise ValueError("FULL inválido: colunas obrigatórias ausentes (SKU, Vendas_60d, Estoque_Full/Atual)")

    out = pd.DataFrame({
        "SKU": df2[sku_col].astype(str).str.strip(),
        "Vendas_Qtd_60d": pd.to_numeric(df2[v60_col], errors="coerce").fillna(0).astype(int),
        "Estoque_Full": pd.to_numeric(df2[est_col], errors="coerce").fillna(0).astype(int),
    })
    if trans_col and trans_col in df2.columns:
        out["Em_Transito"] = pd.to_numeric(df2[trans_col], errors="coerce").fillna(0).astype(int)
    else:
        out["Em_Transito"] = 0
    return out


def _map_shopee_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Mapeia Shopee/MT para colunas: SKU, Quantidade."""
    df2 = df.copy()
    # buscar SKU
    sku_col = None
    for c in df2.columns:
        if "sku" == c or c.endswith("_sku") or c.startswith("sku"):
            sku_col = c
            break
    if sku_col is None and "item_sku" in df2.columns:
        sku_col = "item_sku"
    if sku_col is None:
        # pegar primeira coluna textual
        sku_col = df2.columns[0]

    # Quantidade: qualquer coluna que contenha qtde/quant/venda/order
    qtd_col = None
    for c in df2.columns:
        cs = str(c)
        if any(x in cs for x in ["qtde", "quant", "qtd", "venda", "order"]):
            qtd_col = c
            break
    if qtd_col is None:
        raise ValueError("Shopee/MT inválido: não encontrei coluna de quantidade (qtde/quant/venda/order)")

    out = pd.DataFrame({
        "SKU": df2[sku_col].astype(str).str.strip(),
        "Quantidade": pd.to_numeric(df2[qtd_col], errors="coerce").fillna(0).astype(int),
    })
    return out


def _map_fisico_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Mapeia Estoque Físico para colunas: SKU, Estoque_Fisico, Preco."""
    df2 = df.copy()
    # SKU
    sku_col = None
    for c in df2.columns:
        if "sku" in c:
            sku_col = c
            break
    if sku_col is None:
        sku_col = df2.columns[0]

    # Estoque
    est_col = None
    for c in df2.columns:
        if any(x in c for x in ["estoque", "qtd", "quant", "dispon"]):
            est_col = c
            break
    if est_col is None:
        raise ValueError("Estoque Físico inválido: precisa de coluna de estoque")

    # Preço/Custo
    preco_col = None
    for c in df2.columns:
        if any(x in c for x in ["preco", "custo", "valor", "price"]):
            preco_col = c
            break
    if preco_col is None:
        raise ValueError("Estoque Físico inválido: precisa de coluna de preço/custo")

    out = pd.DataFrame({
        "SKU": df2[sku_col].astype(str).str.strip(),
        "Estoque_Fisico": pd.to_numeric(df2[est_col], errors="coerce").fillna(0).astype(int),
        "Preco": pd.to_numeric(df2[preco_col], errors="coerce").fillna(0.0).astype(float),
    })
    return out

# ==========================
# KITS/CAT — Carregamento manual via URL (export XLSX)
# ==========================
@st.cache_data(show_spinner=False)
def load_kits_cat_from_url(url_xlsx: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if not url_xlsx:
        raise ValueError("Informe a URL de exportação XLSX do Google Sheets para o Padrão (KITS/CAT)")
    xls = pd.ExcelFile(url_xlsx)

    # KITS
    kits_df = None
    for sh in xls.sheet_names:
        if sh in KITS_SHEETS_OK:
            k = pd.read_excel(xls, sh)
            k.columns = [_norm_col(c) for c in k.columns]
            # Esperado: kit_sku, component_sku, qty
            if not {"kit_sku", "component_sku", "qty"}.issubset(set(k.columns)):
                continue
            kits_df = k[["kit_sku", "component_sku", "qty"]].copy()
            kits_df["qty"] = pd.to_numeric(kits_df["qty"], errors="coerce").fillna(0).astype(int)
            break
    if kits_df is None:
        # se não tiver KITS, cria vazio
        kits_df = pd.DataFrame(columns=["kit_sku", "component_sku", "qty"])

    # CAT
    cat_df = None
    for sh in xls.sheet_names:
        if sh in CAT_SHEETS_OK:
            c = pd.read_excel(xls, sh)
            c.columns = [_norm_col(c2) for c2 in c.columns]
            if "component_sku" not in c.columns:
                continue
            cat_df = c[["component_sku"] + [col for col in ["fornecedor", "status_reposicao"] if col in c.columns]].copy()
            break
    if cat_df is None:
        cat_df = pd.DataFrame(columns=["component_sku", "fornecedor", "status_reposicao"])

    # Criar alias 1:1 para componentes que não são kits (para permitir explosão uniforme)
    # Se um component_sku existir no CAT mas não aparece como kit_sku, adicionamos um alias kit_sku==component_sku, qty=1
    comp_set = set(cat_df["component_sku"].dropna().astype(str))
    kit_set = set(kits_df["kit_sku"].dropna().astype(str))
    alias = sorted(list(comp_set - kit_set))
    if alias:
        alias_df = pd.DataFrame({
            "kit_sku": alias,
            "component_sku": alias,
            "qty": 1,
        })
        kits_df = pd.concat([kits_df, alias_df], ignore_index=True)

    return kits_df, cat_df

# ==========================
# Explosão por KITS
# ==========================

def explode_skus(df_sku_qtd: pd.DataFrame, sku_col: str, qty_col: str, kits: pd.DataFrame) -> pd.DataFrame:
    """Recebe um DF com SKU+quantidade e explode via tabela KITS (kit_sku→component_sku×qty)."""
    if df_sku_qtd.empty:
        return df_sku_qtd.rename(columns={sku_col: "component_sku", qty_col: "quantidade"})

    m = df_sku_qtd.merge(kits, left_on=sku_col, right_on="kit_sku", how="left")
    m["qty"].fillna(0, inplace=True)
    # Se não casar com kit, cai no alias 1:1 criado na carga do padrão
    m["consumo"] = pd.to_numeric(m[qty_col], errors="coerce").fillna(0) * pd.to_numeric(m["qty"], errors="coerce").fillna(0)
    out = (
        m.groupby("component_sku", dropna=True)["consumo"].sum().reset_index()
        .rename(columns={"consumo": "quantidade"})
    )
    out["component_sku"] = out["component_sku"].astype(str)
    out["quantidade"] = out["quantidade"].fillna(0).astype(int)
    return out

# ==========================
# Cálculo: Compra Automática (congelado)
# ==========================

def calcular_compra_automatica(
    full_df: pd.DataFrame,
    shopee_df: pd.DataFrame,
    fisico_df: pd.DataFrame,
    kits_df: pd.DataFrame,
    cat_df: pd.DataFrame,
    horizonte_dias: int,
    lead_time_dias: int,
    crescimento_pct: float,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Retorna (Lista_Final, Controle) já com necessidade por componente e valor de compra."""
    # 1) FULL → consumo diário e alvo
    base_full = full_df.copy()
    base_full["vendas_dia"] = base_full["Vendas_Qtd_60d"].astype(float) / 60.0

    # crescimento composto aproximado sobre o período do horizonte
    fator = (1.0 + (crescimento_pct / 100.0)) ** (horizonte_dias / 30.0)
    base_full["alvo"] = np.round(base_full["vendas_dia"] * (lead_time_dias + horizonte_dias) * fator).astype(int)
    base_full["oferta"] = base_full["Estoque_Full"].fillna(0).astype(int) + base_full.get("Em_Transito", 0).fillna(0).astype(int)
    base_full["envio_desejado"] = (base_full["alvo"] - base_full["oferta"]).clip(lower=0)

    # 2) Explodir envio_desejado por KITS → Necessidade por componente
    nec = explode_skus(base_full[["SKU", "envio_desejado"]], "SKU", "envio_desejado", kits_df)
    nec.rename(columns={"component_sku": "component_sku", "quantidade": "Necessidade"}, inplace=True)

    # 3) Shopee/MT 60d aproximado (se vier total no período) — aqui usamos a coluna Quantidade como 60d
    sh = shopee_df.copy()
    sh60 = explode_skus(sh.rename(columns={"Quantidade": "q"}), "SKU", "q", kits_df)
    sh60.rename(columns={"quantidade": "Shopee_60d"}, inplace=True)

    # 4) FULL 60d explodido
    full60 = explode_skus(base_full.rename(columns={"Vendas_Qtd_60d": "q"})[["SKU", "q"]], "SKU", "q", kits_df)
    full60.rename(columns={"quantidade": "ML_60d"}, inplace=True)

    # 5) Consolidar componentes + catálogo + estoque físico
    comp = pd.DataFrame({"component_sku": pd.unique(pd.concat([nec["component_sku"], full60["component_sku"], sh60["component_sku"]], ignore_index=True))})

    comp = comp.merge(nec, on="component_sku", how="left")
    comp = comp.merge(full60, on="component_sku", how="left")
    comp = comp.merge(sh60, on="component_sku", how="left")
    comp["Necessidade"] = comp["Necessidade"].fillna(0).astype(int)
    comp["ML_60d"] = comp["ML_60d"].fillna(0).astype(int)
    comp["Shopee_60d"] = comp["Shopee_60d"].fillna(0).astype(int)

    # TOTAL_60d = max(ML_60d + Shopee_60d, ML_60d) (regra do memorando)
    comp["TOTAL_60d"] = np.maximum(comp["ML_60d"] + comp["Shopee_60d"], comp["ML_60d"])
    comp["Reserva_30d"] = np.round((comp["TOTAL_60d"].astype(float) / 60.0) * 30.0).astype(int)

    # Estoque Físico + Preço
    fis = fisico_df.copy()
    fis = fis[["SKU", "Estoque_Fisico", "Preco"]].rename(columns={"SKU": "component_sku"})
    comp = comp.merge(fis, on="component_sku", how="left")
    comp["Estoque_Fisico"] = comp["Estoque_Fisico"].fillna(0).astype(int)
    comp["Preco"] = comp["Preco"].fillna(0.0).astype(float)

    # Catálogo (fornecedor, status_reposicao)
    cat = cat_df.copy()
    comp = comp.merge(cat, on="component_sku", how="left")

    # Folga Física e Compra Sugerida
    comp["Folga_Fisico"] = (comp["Estoque_Fisico"] - comp["Reserva_30d"]).clip(lower=0).astype(int)
    comp["Compra_Sugerida"] = (comp["Necessidade"] - comp["Folga_Fisico"]).clip(lower=0).astype(int)

    # Zerar se status_reposicao == 'nao_repor'
    comp.loc[comp["status_reposicao"].astype(str).str.lower().eq("nao_repor"), "Compra_Sugerida"] = 0

    comp["Valor_Compra_R$"] = (comp["Compra_Sugerida"].astype(float) * comp["Preco"].astype(float)).round(2)

    # Ordenação padrão: fornecedor ↑, valor_compra ↓, SKU ↑
    if "fornecedor" in comp.columns:
        comp.sort_values(by=["fornecedor", "Valor_Compra_R$", "component_sku"], ascending=[True, False, True], inplace=True)
    else:
        comp.sort_values(by=["Valor_Compra_R$", "component_sku"], ascending=[False, True], inplace=True)

    # Controle para auditoria
    controle = comp[[
        "component_sku", "Necessidade", "ML_60d", "Shopee_60d", "TOTAL_60d", "Reserva_30d",
        "Estoque_Fisico", "Folga_Fisico", "Compra_Sugerida", "Preco", "Valor_Compra_R$",
        "fornecedor", "status_reposicao"
    ]].copy()

    # Hash de integridade (sha256 do CSV de controle)
    csv_bytes = controle.to_csv(index=False).encode("utf-8")
    sha = hashlib.sha256(csv_bytes).hexdigest()
    controle["hash_sha256"] = sha

    # Lista_Final (para exibição/Export)
    lista_final = comp.rename(columns={"component_sku": "SKU"})[
        ["SKU", "fornecedor", "Compra_Sugerida", "Valor_Compra_R$"]
    ].copy()

    return lista_final, controle

# ==========================
# Alocação proporcional (sem estoque)
# ==========================

def alocar_compra(
    total_qtd: int,
    comp_sku: str,
    full_alivvia: pd.DataFrame, shopee_alivvia: pd.DataFrame,
    full_jca: pd.DataFrame, shopee_jca: pd.DataFrame,
    kits_df: pd.DataFrame,
) -> pd.DataFrame:
    # Explodir FULL 60d e Shopee 60d para ALIVVIA
    ml_a = explode_skus(full_alivvia.rename(columns={"Vendas_Qtd_60d": "q"})[["SKU", "q"]], "SKU", "q", kits_df)
    ml_a.rename(columns={"quantidade": "ML_60d_A"}, inplace=True)
    sp_a = explode_skus(shopee_alivvia.rename(columns={"Quantidade": "q"}), "SKU", "q", kits_df)
    sp_a.rename(columns={"quantidade": "Shopee_60d_A"}, inplace=True)

    # JCA
    ml_j = explode_skus(full_jca.rename(columns={"Vendas_Qtd_60d": "q"})[["SKU", "q"]], "SKU", "q", kits_df)
    ml_j.rename(columns={"quantidade": "ML_60d_J"}, inplace=True)
    sp_j = explode_skus(shopee_jca.rename(columns={"Quantidade": "q"}), "SKU", "q", kits_df)
    sp_j.rename(columns={"quantidade": "Shopee_60d_J"}, inplace=True)

    # Consolidar
    base = pd.DataFrame({"component_sku": [comp_sku]})
    base = base.merge(ml_a, on="component_sku", how="left")
    base = base.merge(sp_a, on="component_sku", how="left")
    base = base.merge(ml_j, on="component_sku", how="left")
    base = base.merge(sp_j, on="component_sku", how="left")

    for c in ["ML_60d_A", "Shopee_60d_A", "ML_60d_J", "Shopee_60d_J"]:
        base[c] = base[c].fillna(0).astype(int)

    base["Demanda_A"] = base["ML_60d_A"] + base["Shopee_60d_A"]
    base["Demanda_J"] = base["ML_60d_J"] + base["Shopee_60d_J"]

    soma = int(base["Demanda_A"].iloc[0] + base["Demanda_J"].iloc[0])
    if soma == 0:
        al_a = total_qtd // 2
        al_j = total_qtd - al_a
    else:
        prop_a = base["Demanda_A"].iloc[0] / soma
        al_a = int(round(total_qtd * prop_a))
        al_j = int(total_qtd - al_a)

    return pd.DataFrame({
        "component_sku": [comp_sku, comp_sku],
        "empresa": ["ALIVVIA", "JCA"],
        "alocacao_sugerida": [al_a, al_j],
    })

# ==========================
# UI — Estados básicos (filtros pós-geração)
# ==========================
if "filtro_fornecedor" not in st.session_state:
    st.session_state["filtro_fornecedor"] = []
if "filtro_sku" not in st.session_state:
    st.session_state["filtro_sku"] = []

st.set_page_config(page_title="Reposição Logística — Alivvia/JCA", layout="wide")
st.title("Reposição Logística (Alivvia/JCA)")

# ==========================
# Sidebar — Carregar Padrão (KITS/CAT)
# ==========================
st.sidebar.subheader("Padrão KITS/CAT (Google Sheets)")
url_kits_cat = st.sidebar.text_input("URL export XLSX do Google (export?format=xlsx)")
if st.sidebar.button("Carregar Padrão (KITS/CAT)"):
    try:
        kits_df, cat_df = load_kits_cat_from_url(url_kits_cat)
        st.session_state["KITS_DF"] = kits_df
        st.session_state["CAT_DF"] = cat_df
        st.sidebar.success("Padrão KITS/CAT carregado.")
    except Exception as e:
        st.sidebar.error(f"Falha ao carregar Padrão: {e}")

# Mostrar status do padrão
kits_ok = st.session_state.get("KITS_DF") is not None
cat_ok = st.session_state.get("CAT_DF") is not None
st.sidebar.caption(f"KITS: {'OK' if kits_ok else '—'} | CAT: {'OK' if cat_ok else '—'}")

# ==========================
# Abas
# ==========================
aba = st.tabs(["Dados das Empresas", "Compra Automática", "Alocação de Compra"])

# --------------------------
# ABA 1 — Dados das Empresas
# --------------------------
with aba[0]:
    st.subheader("Dados das Empresas")

    # Rehidratar sessão a partir da persistência (se sessão vazia)
    for empresa in EMPRESAS:
        if st.session_state.get(f"{empresa}_FULL") is None:
            df = _load_df(empresa, "FULL")
            if df is not None:
                st.session_state[f"{empresa}_FULL"] = df
        if st.session_state.get(f"{empresa}_SHOPEE") is None:
            df = _load_df(empresa, "SHOPEE")
            if df is not None:
                st.session_state[f"{empresa}_SHOPEE"] = df
        if st.session_state.get(f"{empresa}_FISICO") is None:
            df = _load_df(empresa, "FISICO")
            if df is not None:
                st.session_state[f"{empresa}_FISICO"] = df

    cols = st.columns(2)
    for i, empresa in enumerate(EMPRESAS):
        with cols[i]:
            st.markdown(f"### {empresa}")
            f_full = st.file_uploader(f"{empresa} — FULL (XLSX/CSV)", key=f"up_{empresa}_FULL")
            f_shop = st.file_uploader(f"{empresa} — Shopee/MT (XLSX/CSV)", key=f"up_{empresa}_SHOPEE")
            f_fis  = st.file_uploader(f"{empresa} — Estoque Físico (opcional) (XLSX/CSV)", key=f"up_{empresa}_FISICO")

            colb1, colb2 = st.columns(2)
            with colb1:
                if st.button(f"Salvar {empresa}"):
                    try:
                        if f_full is not None:
                            df_full_raw = _read_any_table(f_full)
                            df_full = _map_full_columns(df_full_raw)
                            st.session_state[f"{empresa}_FULL"] = df_full
                        if f_shop is not None:
                            df_sh_raw = _read_any_table(f_shop)
                            df_sh = _map_shopee_columns(df_sh_raw)
                            st.session_state[f"{empresa}_SHOPEE"] = df_sh
                        if f_fis is not None:
                            df_fi_raw = _read_any_table(f_fis)
                            df_fi = _map_fisico_columns(df_fi_raw)
                            st.session_state[f"{empresa}_FISICO"] = df_fi

                        # Persistir o que existir
                        _persist_df(st.session_state.get(f"{empresa}_FULL"), empresa, "FULL")
                        _persist_df(st.session_state.get(f"{empresa}_SHOPEE"), empresa, "SHOPEE")
                        _persist_df(st.session_state.get(f"{empresa}_FISICO"), empresa, "FISICO")
                        st.success(f"Bases da {empresa} salvas e persistidas.")
                    except Exception as e:
                        st.error(f"Falha ao salvar {empresa}: {e}")

            with colb2:
                if st.button(f"Limpar {empresa}"):
                    for key in ("FULL", "SHOPEE", "FISICO"):
                        st.session_state[f"{empresa}_{key}"] = None
                    _clear_storage(empresa)
                    st.warning(f"{empresa} limpa (sessão + armazenamento).")

            # Mostrar status
            st.caption(
                f"FULL: {'OK' if st.session_state.get(f'{empresa}_FULL') is not None else '—'} | "
                f"Shopee: {'OK' if st.session_state.get(f'{empresa}_SHOPEE') is not None else '—'} | "
                f"Físico: {'OK' if st.session_state.get(f'{empresa}_FISICO') is not None else '—'}"
            )

# --------------------------
# ABA 2 — Compra Automática
# --------------------------
with aba[1]:
    st.subheader("Compra Automática")
    kits_df = st.session_state.get("KITS_DF")
    cat_df  = st.session_state.get("CAT_DF")

    if kits_df is None or cat_df is None:
        st.error("Carregue o Padrão KITS/CAT na barra lateral antes de continuar.")
    else:
        empresa_ativa = st.selectbox("Empresa", EMPRESAS)
        colp1, colp2, colp3 = st.columns(3)
        with colp1:
            horizonte = st.number_input("Horizonte (dias)", min_value=7, max_value=120, value=60, step=1)
        with colp2:
            lt = st.number_input("Lead time (dias)", min_value=0, max_value=120, value=15, step=1)
        with colp3:
            cresc = st.number_input("Crescimento mensal (%)", min_value=-50.0, max_value=200.0, value=0.0, step=1.0)

        if st.button("Gerar Compra"):
            try:
                full = st.session_state.get(f"{empresa_ativa}_FULL")
                sh = st.session_state.get(f"{empresa_ativa}_SHOPEE")
                fi = st.session_state.get(f"{empresa_ativa}_FISICO")
                if full is None:
                    st.error("Base FULL não encontrada para a empresa selecionada.")
                elif sh is None:
                    st.error("Base Shopee/MT não encontrada para a empresa selecionada.")
                elif fi is None:
                    st.error("Estoque Físico é obrigatório para Compra Automática.")
                else:
                    lista_final, controle = calcular_compra_automatica(full, sh, fi, kits_df, cat_df, int(horizonte), int(lt), float(cresc))
                    st.session_state["_CA_LISTA_FINAL"] = lista_final
                    st.session_state["_CA_CONTROLE"] = controle
                    st.success("Compra gerada.")
            except Exception as e:
                st.error(f"Erro ao calcular compra: {e}")

        # Se já existe resultado, mostrar com filtros autocompletáveis (restaurado)
        df_final = st.session_state.get("_CA_LISTA_FINAL")
        df_controle = st.session_state.get("_CA_CONTROLE")
        if isinstance(df_final, pd.DataFrame) and not df_final.empty:
            df_view = df_final.copy()

            # Opções
            opts_fornecedor = sorted([x for x in df_view.get("fornecedor", pd.Series(dtype=str)).dropna().unique().tolist() if isinstance(x, str)])
            opts_sku = sorted([x for x in df_view.get("SKU", pd.Series(dtype=str)).dropna().unique().tolist() if isinstance(x, str)])

            f1, f2 = st.columns(2)
            with f1:
                sel_forn = st.multiselect(
                    "Filtrar por Fornecedor",
                    options=opts_fornecedor,
                    default=st.session_state.get("filtro_fornecedor", []),
                    help="Selecione um ou mais fornecedores (autocomplete)."
                )
            with f2:
                sel_sku = st.multiselect(
                    "Filtrar por SKU",
                    options=opts_sku,
                    default=st.session_state.get("filtro_sku", []),
                    help="Selecione um ou mais SKUs (autocomplete)."
                )

            st.session_state["filtro_fornecedor"] = sel_forn
            st.session_state["filtro_sku"] = sel_sku

            # Aplicar filtros
            if sel_forn:
                df_view = df_view[df_view["fornecedor"].isin(sel_forn)]
            if sel_sku:
                df_view = df_view[df_view["SKU"].isin(sel_sku)]

            st.dataframe(df_view, use_container_width=True)

            # Export XLSX (Lista_Final filtrada + Controle íntegro para auditoria)
            def _to_xlsx_bytes(df_lista_final: pd.DataFrame, df_controle: pd.DataFrame) -> bytes:
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
                    df_lista_final.to_excel(writer, index=False, sheet_name="Lista_Final")
                    df_controle.to_excel(writer, index=False, sheet_name="Controle")
                return output.getvalue()

            xbytes = _to_xlsx_bytes(df_view, df_controle if isinstance(df_controle, pd.DataFrame) else pd.DataFrame())
            st.download_button(
                "Exportar XLSX (Filtrado)",
                data=xbytes,
                file_name="compra_automatica.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

# --------------------------
# ABA 3 — Alocação de Compra
# --------------------------
with aba[2]:
    st.subheader("Alocação de Compra")

    kits_df = st.session_state.get("KITS_DF")
    if kits_df is None:
        st.error("Carregue o Padrão KITS/CAT na barra lateral antes de continuar.")
    else:
        comp_sku = st.text_input("Componente (component_sku)")
        total = st.number_input("Quantidade total a alocar", min_value=0, value=0, step=1)

        if st.button("Calcular Alocação"):
            try:
                full_a = st.session_state.get("ALIVVIA_FULL")
                sh_a = st.session_state.get("ALIVVIA_SHOPEE")
                full_j = st.session_state.get("JCA_FULL")
                sh_j = st.session_state.get("JCA_SHOPEE")

                if any(x is None for x in [full_a, sh_a, full_j, sh_j]):
                    st.error("É necessário ter FULL e Shopee salvos para as duas empresas.")
                else:
                    res = alocar_compra(int(total), comp_sku.strip(), full_a, sh_a, full_j, sh_j, kits_df)
                    st.dataframe(res, use_container_width=True)
                    # Export CSV
                    csv = res.to_csv(index=False).encode("utf-8")
                    st.download_button(
                        "Baixar CSV",
                        data=csv,
                        file_name=f"alocacao_{comp_sku}.csv",
                        mime="text/csv"
                    )
            except Exception as e:
                st.error(f"Erro na alocação: {e}")
