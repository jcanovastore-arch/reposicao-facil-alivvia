"""
ORION.ui.components — filtros, busca inteligente e grade com seleção persistente
(ATHENAS: preparar pacote de itens para Ordem de Compra)
"""
from __future__ import annotations
import numpy as np
import pandas as pd
import streamlit as st

def filtro_texto_inteligente(df: pd.DataFrame, texto: str, colunas_busca: list[str]) -> pd.DataFrame:
    if not texto:
        return df
    termos = [t.strip() for t in str(texto).split() if t.strip()]
    if not termos:
        return df
    base = df.copy()
    for col in colunas_busca:
        if col not in base.columns:
            base[col] = ""
        base[col] = base[col].astype(str)
    mask_total = np.ones(len(base), dtype=bool)
    for termo in termos:
        termo_up = termo.upper()
        mask_termo = np.zeros(len(base), dtype=bool)
        for col in colunas_busca:
            mask_termo |= base[col].str.upper().str.contains(termo_up, na=False)
        mask_total &= mask_termo
    return base[mask_total]

def preparar_df_para_oc(base: pd.DataFrame) -> pd.DataFrame:
    df = base.copy()
    if "Descricao" not in df.columns:
        df["Descricao"] = df["SKU"]
    df["Qtd"] = pd.to_numeric(df.get("Compra_Sugerida", 0), errors="coerce").fillna(0).astype(float)
    df["PrecoUnit"] = pd.to_numeric(df.get("Preco", 0.0), errors="coerce").fillna(0.0).astype(float)
    keep = ["SKU", "Descricao", "Qtd", "PrecoUnit"]
    if "fornecedor" in df.columns:
        keep.append("fornecedor")
    df = df[keep].copy()
    df = df[df["Qtd"] > 0]
    df["SKU"] = df["SKU"].astype(str).str.strip().str.upper()
    df["Descricao"] = df["Descricao"].astype(str).str.strip()
    return df.reset_index(drop=True)

def bloco_filtros_e_selecao(df_final: pd.DataFrame, empresa: str, state_key_prefix: str = "oc") -> tuple[pd.DataFrame, set]:
    with st.expander("Filtros (após geração) — sem recálculo", expanded=True):
        colf1, colf2, colf3 = st.columns([1,1,1])
        fornecedores = sorted([f for f in df_final.get("fornecedor", pd.Series([], dtype=str)).dropna().astype(str).unique().tolist() if f != ""])
        sel_fornec = colf1.multiselect("Fornecedor", options=fornecedores, default=[], key=f"{state_key_prefix}_f_fornec_{empresa}")
        sku_all = sorted(df_final["SKU"].dropna().astype(str).unique().tolist())
        sku_opts = ["(todos)"] + sku_all
        sku_auto = colf2.selectbox("SKU (auto-completar)", options=sku_opts, index=0, key=f"{state_key_prefix}_f_sku_auto_{empresa}")
        busca_txt = colf3.text_input("Busca inteligente (SKU/Fornecedor)", key=f"{state_key_prefix}_f_busca_{empresa}", placeholder="Ex.: 404 PRETO, HIDRO, MINI…")

    df_view = df_final.copy()
    if sel_fornec:
        df_view = df_view[df_view["fornecedor"].isin(sel_fornec)]
    if sku_auto and sku_auto != "(todos)":
        df_view = df_view[df_view["SKU"].astype(str).str.upper() == str(sku_auto).upper()]
    if busca_txt:
        df_view = filtro_texto_inteligente(df_view, busca_txt, ["SKU", "fornecedor"])

    if "TOTAL_60d" not in df_view.columns and {"ML_60d","Shopee_60d"}.issubset(df_view.columns):
        df_view["TOTAL_60d"] = df_view[["ML_60d", "Shopee_60d"]].sum(axis=1)

    cols_show = [
        "fornecedor", "SKU",
        "Vendas_h_Shopee", "Vendas_h_ML", "TOTAL_60d",
        "Estoque_Fisico", "Preco",
        "Compra_Sugerida", "Valor_Compra_R$",
    ]
    df_view = df_view[[c for c in cols_show if c in df_view.columns]].copy()

    tot_compra = float(df_view.get("Valor_Compra_R$", pd.Series(dtype=float)).sum())
    tot_qtd = int(pd.to_numeric(df_view.get("Compra_Sugerida", pd.Series(dtype=float)), errors="coerce").fillna(0).sum())
    colm1, colm2 = st.columns([1,1])
    colm1.info(f"**Total Compra Sugerida (un):** {tot_qtd:,}".replace(",", "."))
    colm2.info(f"**Valor Total (R$) do conjunto filtrado:** R$ {tot_compra:,.2f}")

    st.caption("Marque os SKUs que deseja **enviar para a Ordem de Compra**. A seleção fica salva por SKU (independe dos filtros).")
    st.session_state.setdefault(f"{state_key_prefix}_selection", {})
    sel_set = st.session_state[f"{state_key_prefix}_selection"].setdefault(empresa, set())

    col_bulk1, col_bulk2, col_bulk3 = st.columns([1,1,2])
    with col_bulk1:
        if st.button("Selecionar TODOS os visíveis", key=f"{state_key_prefix}_sel_all_vis_{empresa}"):
            sel_set |= set(df_view["SKU"].astype(str))
            st.session_state[f"{state_key_prefix}_selection"][empresa] = sel_set
    with col_bulk2:
        if st.button("Limpar seleção (visíveis)", key=f"{state_key_prefix}_clear_vis_{empresa}"):
            sel_set -= set(df_view["SKU"].astype(str))
            st.session_state[f"{state_key_prefix}_selection"][empresa] = sel_set
    with col_bulk3:
        sku_list_all = sorted(df_final["SKU"].dropna().astype(str).unique().tolist())
        sku_pick = st.selectbox("Adicionar 1 SKU por auto-completar", ["(digite para buscar)"] + sku_list_all,
                                index=0, key=f"{state_key_prefix}_add_sku_auto_{empresa}")
        if sku_pick and sku_pick != "(digite para buscar)":
            sel_set.add(str(sku_pick).upper())
            st.session_state[f"{state_key_prefix}_selection"][empresa] = sel_set
            st.experimental_rerun()

    df_show = df_view.copy()
    df_show.insert(0, "Selecionar", df_show["SKU"].isin(sel_set))

    edited = st.data_editor(
        df_show,
        use_container_width=True,
        height=520,
        hide_index=True,
        column_config={
            "Selecionar": st.column_config.CheckboxColumn(label="Selecionar", help="Marque para incluir na OC"),
            "Preco": st.column_config.NumberColumn(format="R$ %.2f"),
            "Valor_Compra_R$": st.column_config.NumberColumn(format="R$ %.2f"),
        },
        disabled=[c for c in df_show.columns if c not in ("Selecionar",)],
        key=f"{state_key_prefix}_grid_{empresa}"
    )

    marcados_vis = set(edited.loc[edited["Selecionar"] == True, "SKU"].astype(str).tolist())
    apareceram = set(df_view["SKU"].astype(str).tolist())
    sel_set |= marcados_vis
    sel_set -= (apareceram - marcados_vis)
    st.session_state[f"{state_key_prefix}_selection"][empresa] = sel_set

    return df_view, sel_set
