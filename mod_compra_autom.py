# mod_compra_autom.py - MÓDULO DA TAB 2 - FIX V8.4
# Responsável por toda a UI, lógica de persistência e filtros da aba "Compra Automática".
# Inclui correções defensivas para o st.data_editor.

import pandas as pd
import streamlit as st
import logica_compra
import numpy as np

from logica_compra import (
    Catalogo,
    aggregate_data_for_conjunta_clean,
    load_any_table_from_bytes,
    mapear_colunas,
    mapear_tipo,
    exportar_xlsx,
    calcular as calcular_compra
)

def render_tab2(state, h, g, LT):
    """Renderiza toda a aba 'Compra Automática'."""
    st.subheader("Gerar Compra (por empresa ou conjunta) — lógica original")

    if state.catalogo_df is None or state.kits_df is None:
        st.info("Carregue o **Padrão (KITS/CAT)** no sidebar antes de usar as abas.")
        return

    # 1. Seleção de Empresa/Conjunta
    # Use o estado globalizado para obter os parâmetros h, g, LT
    empresa_selecionada = st.radio("Empresa ativa", ["ALIVVIA", "JCA", "CONJUNTA"], horizontal=True, key="empresa_ca")
    nome_estado = empresa_selecionada
    
    # Lógica de validação visual
    if nome_estado == "CONJUNTA":
        st.info("Arquivos agregados prontos para o cálculo Conjunto.")
    else:
        dados_display = state[nome_estado]
        col = st.columns(3)
        col[0].info(f"FULL: {dados_display['FULL']['name'] or '—'}")
        col[1].info(f"Shopee/MT: {dados_display['VENDAS']['name'] or '—'}")
        col[2].info(f"Estoque: {dados_display['ESTOQUE']['name'] or '—'}")

    # 2. Lógica de Disparo (ou manutenção do estado)
    if st.button(f"Gerar Compra — {nome_estado}", type="primary"):
        state.compra_autom_data["force_recalc"] = True
    
    # Se o cálculo não existir no estado ou se for forçado, execute-o
    if nome_estado not in state.compra_autom_data or state.compra_autom_data.get("force_recalc", False):
        
        state.compra_autom_data["force_recalc"] = False
        
        # BLOCO DE CÁLCULO
        try:
            cat = Catalogo(
                catalogo_simples=state.catalogo_df.rename(columns={"sku":"component_sku"}),
                kits_reais=state.kits_df
            )

            if nome_estado == "CONJUNTA":
                
                dfs = {}
                missing_conjunta_calc = []
                for emp in ["ALIVVIA", "JCA"]:
                    dados = state[emp]
                    for k, rot in [("FULL", "FULL"), ("VENDAS", "Shopee/MT"), ("ESTOQUE", "Estoque")]:
                        if not (dados[k]["name"] and dados[k]["bytes"]):
                            missing_conjunta_calc.append(f"{emp} {rot}")
                        
                        raw = load_any_table_from_bytes(dados[k]["name"], dados[k]["bytes"])
                        tipo = mapear_tipo(raw)
                        if tipo == "FULL": dfs[f"full_{emp[0]}"] = mapear_colunas(raw, tipo)
                        elif tipo == "VENDAS": dfs[f"vend_{emp[0]}"] = mapear_colunas(raw, tipo)
                        elif tipo == "FISICO": dfs[f"fisi_{emp[0]}"] = mapear_colunas(raw, tipo)
                        else: raise RuntimeError(f"Arquivo {rot} de {emp} com formato incorreto: {tipo}.")

                if missing_conjunta_calc:
                    raise RuntimeError("Arquivos necessários para Compra Conjunta estão ausentes (recarregue todos na aba 'Dados das Empresas').")
                
                full_df, fisico_df, vendas_df = aggregate_data_for_conjunta_clean(
                    dfs['full_A'], dfs['vend_A'], dfs['fisi_A'],
                    dfs['full_J'], dfs['vend_J'], dfs['fisi_J']
                )
                nome_empresa_calc = "CONJUNTA"
                
            else: # Individual (ALIVVIA ou JCA)
                dados = state[nome_estado]
                for k, rot in [("FULL","FULL"),("VENDAS","Shopee/MT"),("ESTOQUE","Estoque")]:
                    if not (dados[k]["name"] and dados[k]["bytes"]):
                        raise RuntimeError(f"Arquivo '{rot}' não foi salvo para {nome_estado}. Vá em **Dados das Empresas** e salve.")
                        
                full_raw   = load_any_table_from_bytes(dados["FULL"]["name"], dados["FULL"]["bytes"])
                vendas_raw = load_any_table_from_bytes(dados["VENDAS"]["name"], dados["VENDAS"]["bytes"])
                fisico_raw = load_any_table_from_bytes(dados["ESTOQUE"]["name"], dados["ESTOQUE"]["bytes"])
                
                t_full = mapear_tipo(full_raw); t_v = mapear_tipo(vendas_raw); t_f = mapear_tipo(fisico_raw)
                if t_full != "FULL" or t_v != "VENDAS" or t_f != "FISICO":
                     raise RuntimeError("Um ou mais arquivos (FULL/VENDAS/FISICO) estão com formato incorreto.")

                full_df   = mapear_colunas(full_raw, t_full)
                vendas_df = mapear_colunas(vendas_raw, t_v)
                fisico_df = mapear_colunas(fisico_raw, t_f)
                nome_empresa_calc = nome_estado

            # 2. CÁLCULO PRINCIPAL
            df_final, painel = calcular_compra(full_df, fisico_df, vendas_df, cat, h=h, g=g, LT=LT)
            
            df_final["Selecionar"] = False # Adiciona coluna de seleção
            
            # SALVA NO ESTADO (CACHING)
            state.compra_autom_data[nome_estado] = {
                "df": df_final,
                "painel": painel,
                "empresa": nome_empresa_calc
            }
            
            st.success("Cálculo concluído. Selecione itens abaixo para Ordem de Compra.")

        except Exception as e:
            state.compra_autom_data[nome_estado] = {"error": str(e)}
            st.error(str(e))
    
    # 3. RENDERIZAÇÃO DE RESULTADOS (USANDO O ESTADO SALVO)
    if nome_estado in state.compra_autom_data and "df" in state.compra_autom_data[nome_estado]:
        
        data_fixa = state.compra_autom_data[nome_estado]
        df_final = data_fixa["df"].copy()
        painel = data_fixa["painel"]
        nome_empresa_calc = data_fixa["empresa"]
        
        if nome_empresa_calc == "CONJUNTA":
            st.warning("⚠️ Compra Conjunta gerada! Use a aba **'📦 Alocação de Compra'** para fracionar o lote sugerido.")
        
        # Renderização do Painel
        cA, cB, cC, cD = st.columns(4)
        cA.metric("Full (un)",  f"{painel['full_unid']:,}".replace(",", "."))
        cB.metric("Full (R$)",  f"R$ {painel['full_valor']:,.2f}")
        cC.metric("Físico (un)",f"{painel['fisico_unid']:,}".replace(",", "."))
        cD.metric("Físico (R$)",f"R$ {painel['fisico_valor']:,.2f}")

        # FILTROS DINÂMICOS
        c_filtros = st.columns(2)
        
        fornecedores = sorted(df_final["fornecedor"].unique().tolist())
        filtro_forn = c_filtros[0].multiselect("Filtrar Fornecedor", fornecedores)
        
        filtro_sku_text = c_filtros[1].text_input("Buscar SKU/Parte do SKU", key=f"filtro_sku_{nome_estado}").strip()
        
        # Aplicação dos Filtros
        df_filtrado = df_final.copy()

        if filtro_forn:
            df_filtrado = df_filtrado[df_filtrado["fornecedor"].isin(filtro_forn)]

        if filtro_sku_text:
            df_filtrado = df_filtrado[df_filtrado["SKU"].str.contains(filtro_sku_text, case=False)]

        # 5. TABELA COM CHECKBOX (Ticar)
        df_para_editor = df_filtrado[df_filtrado["Compra_Sugerida"] > 0].reset_index(drop=True)
        
        editor_key = f"data_editor_{nome_estado}"
        
        # Inicializa a coluna Selecionar para evitar o crash se o estado for resetado
        if editor_key not in state or not isinstance(state[editor_key], dict):
            state[editor_key] = {"edited_rows": {}, "added_rows": [], "deleted_rows": []}

        st.data_editor(df_para_editor, key=editor_key, use_container_width=True, height=500,
            column_config={
                "Selecionar": st.column_config.CheckboxColumn("Selecionar", default=False)
            })
        
        # 6. LÓGICA DO BOTÃO ENVIAR PARA OC
        df_edited_raw = state[editor_key]
        
        df_selecionados = pd.DataFrame()

        # FIX V8.4: Checagem defensiva contra o crash do data_editor (AttributeError/KeyError)
        try:
            # 1. Recupera o DataFrame base (o que está sendo exibido)
            df_base = df_para_editor.copy()
            
            # 2. Verifica se há edições no estado e se é um dict (o Streamlit retorna um dict com as edições)
            if isinstance(df_edited_raw, dict) and 'edited_rows' in df_edited_raw:
                
                # 3. Aplica as edições (incluindo a seleção)
                # Esta é a lógica mais simples: pega o índice das linhas editadas
                selected_indices = [
                    idx for idx, row_data in df_edited_raw['edited_rows'].items() 
                    if row_data.get('Selecionar', False)
                ]
                
                # Se a coluna 'Selecionar' for editada, ela estará em df_edited_raw['edited_rows']
                df_base.loc[df_base.index.isin(df_edited_raw['edited_rows'].keys()), 'Selecionar'] = [
                    df_edited_raw['edited_rows'][idx]['Selecionar'] 
                    for idx in df_edited_raw['edited_rows'] if 'Selecionar' in df_edited_raw['edited_rows'][idx]
                ]
                
                # 4. Seleciona as linhas que foram ticadas
                df_selecionados = df_base[df_base['Selecionar'] == True].copy()
            
            elif isinstance(df_edited_raw, pd.DataFrame):
                # Fallback: Se por acaso o Streamlit retornou o DF completo (versões antigas/instáveis)
                df_selecionados = df_edited_raw[df_edited_raw["Selecionar"] == True].copy()
                
        except Exception:
            # Em caso de qualquer erro (ex: índice inválido), assume que nada foi selecionado.
            df_selecionados = pd.DataFrame()


        if df_selecionados.empty:
            st.button(f"Enviar 0 itens selecionados para a Cesta de OC", disabled=True)
        else:
            if st.button(f"Enviar {len(df_selecionados)} itens selecionados para a Cesta de OC", type="secondary"):
                df_selecionados["Empresa"] = nome_empresa_calc
                # Garante que só itens com compra sugerida > 0 sejam enviados
                df_selecionados = df_selecionados[df_selecionados["Compra_Sugerida"] > 0]
                
                # Lógica de concatenação e limpeza (mantida)
                if state.get("oc_cesta") is None or state.oc_cesta.empty:
                    state.oc_cesta = df_selecionados
                else:
                    cesta_atual = state.oc_cesta[state.oc_cesta["Empresa"] != nome_empresa_calc].copy()
                    state.oc_cesta = pd.concat([cesta_atual, df_selecionados], ignore_index=True)

                st.success(f"Itens de {nome_empresa_calc} enviados para a Cesta de OC. Total na Cesta: {len(state.oc_cesta)} itens.")
                st.dataframe(state.oc_cesta, use_container_width=True)

        if st.checkbox("Gerar XLSX (Lista_Final + Controle)", key="chk_xlsx"):
            xlsx = exportar_xlsx(df_final, h=h, params={"g":g,"LT":LT,"empresa":nome_empresa_calc})
            st.download_button(
                "Baixar XLSX", data=xlsx,
                file_name=f"Compra_Sugerida_{nome_empresa_calc}_{h}d.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )