# mod_alocacao.py - MÓDULO DA TAB 3
# Responsável por toda a UI e lógica da aba "Alocação de Compra".

import pandas as pd
import streamlit as st
import logica_compra

from logica_compra import (
    Catalogo,
    load_any_table_from_bytes,
    mapear_colunas,
    mapear_tipo,
    calcular_vendas_componente,
    norm_sku
)

def render_tab3(state):
    """Renderiza toda a aba 'Alocação de Compra'."""
    st.subheader("📦 Alocação de Compra — Fracionar Lote por Proporção de Vendas")

    if state.catalogo_df is None or state.kits_df is None:
        st.info("Carregue o **Padrão (KITS/CAT)** no sidebar antes de usar as abas.")
        return

    CATALOGO = state.catalogo_df
    
    missing_data = False
    for emp in ["ALIVVIA", "JCA"]:
        # Requer FULL e VENDAS
        if not (state[emp]["FULL"]["name"] and state[emp]["VENDAS"]["name"]):
             missing_data = True
             break
    
    if missing_data:
        st.warning("É necessário carregar os arquivos **FULL** e **Shopee/MT (Vendas)** para AMBAS as empresas na aba **Dados das Empresas**.")
        return
    
    try:
        def read_pair_alloc_ui(emp: str) -> tuple:
            dados = state[emp]
            fa = load_any_table_from_bytes(dados["FULL"]["name"], dados["FULL"]["bytes"])
            sa = load_any_table_from_bytes(dados["VENDAS"]["name"], dados["VENDAS"]["bytes"])
            
            tfa = mapear_tipo(fa); tsa = mapear_tipo(sa)
            if tfa != "FULL":   raise RuntimeError(f"FULL inválido ({emp}): precisa de SKU e Vendas_60d/Estoque_full.")
            if tsa != "VENDAS": raise RuntimeError(f"Vendas inválido ({emp}): não achei coluna de quantidade.")
            
            return mapear_colunas(fa, tfa), mapear_colunas(sa, tsa)

        full_A, shp_A = read_pair_alloc_ui("ALIVVIA")
        full_J, shp_J = read_pair_alloc_ui("JCA")
        
        cat_obj = Catalogo(
            catalogo_simples=CATALOGO.rename(columns={"sku":"component_sku"}),
            kits_reais=state.kits_df
        )

        # Cálculo das Vendas Agregadas por Componente (Chamada para o MÓDULO DE LÓGICA)
        demA = calcular_vendas_componente(full_A, shp_A, cat_obj).rename(columns={"Demanda_60d":"Demanda_A"})
        demJ = calcular_vendas_componente(full_J, shp_J, cat_obj).rename(columns={"Demanda_60d":"Demanda_J"})
        
        demanda_comp = pd.merge(demA[["SKU", "Demanda_A"]], demJ[["SKU", "Demanda_J"]], on="SKU", how="outer").fillna(0)
        demanda_comp["TOTAL_60d"] = demanda_comp["Demanda_A"] + demanda_comp["Demanda_J"]
        
        # 3. Seleção do SKU e Quantidade
        sku_opcoes = demanda_comp["SKU"].unique().tolist()
        
        col_sel, col_qtd = st.columns([2, 1])
        with col_sel:
            sku_escolhido = st.selectbox("SKU do componente para alocar", sku_opcoes, key="alloc_sku")
        with col_qtd:
            qtd_lote = st.number_input("Quantidade total do lote (ex.: 500 un)", min_value=1, value=1000, step=50, key="alloc_qtd")

        if st.button("Calcular Alocação Proporcional", type="primary"):
            
            sku_norm = norm_sku(sku_escolhido)
            
            # 4. Cálculo da Proporção
            dados_sku = demanda_comp[demanda_comp["SKU"] == sku_norm]
            
            if dados_sku.empty:
                 raise ValueError(f"SKU {sku_norm} não encontrado ou sem demanda nas empresas.")
                 
            dA = dados_sku["Demanda_A"].iloc[0]
            dJ = dados_sku["Demanda_J"].iloc[0]
            total = dA + dJ
            
            if total == 0:
                st.warning("SKU encontrado, mas sem vendas nos últimos 60 dias. Usando alocação 50/50.")
                propA = propJ = 0.5
            else:
                propA = dA / total
                propJ = dJ / total
            
            # 5. Cálculo da Alocação
            alocA = int(round(qtd_lote * propA))
            alocJ = int(qtd_lote - alocA)

            # 6. Output
            res = pd.DataFrame([
                {"Empresa":"ALIVVIA", "SKU":sku_norm, "Demanda_60d":dA, "Proporção":round(propA,4), "Alocação_Sugerida":alocA},
                {"Empresa":"JCA",     "SKU":sku_norm, "Demanda_60d":dJ, "Proporção":round(propJ,4), "Alocação_Sugerida":alocJ},
            ])
            
            st.success(f"Lote de {qtd_lote} unidades de **{sku_norm}** alocado com sucesso.")
            st.dataframe(res, use_container_width=True)
            st.markdown(f"**Resultado:** ALIVVIA recebe **{alocA}** un. ({round(propA*100, 1)}%) | JCA recebe **{alocJ}** un. ({round(propJ*100, 1)}%)")
            st.download_button("Baixar alocação (.csv)", data=res.to_csv(index=False).encode("utf-8"),
                               file_name=f"Alocacao_{sku_norm}_{qtd_lote}.csv", mime="text/csv")
    
    except Exception as e:
        st.error(f"Erro ao calcular Alocação de Compra: {e}")