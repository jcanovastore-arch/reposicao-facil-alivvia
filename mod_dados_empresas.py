# mod_dados_empresas.py - MÓDULO DA TAB 1 - FIX V5.1.1
# FIX: Corrigido NameError (up_file -> up_full)

import streamlit as st
import logica_compra

def render_tab1(state):
    """Renderiza toda a aba 'Dados das Empresas'."""
    st.subheader("Uploads fixos por empresa (os arquivos permanecem salvos após F5)")
    st.caption("Faça o upload e clique em **Salvar [Empresa]** para persistir o estado.")

    def bloco_empresa(emp: str):
        st.markdown(f"### {emp}")
        c1, c2 = st.columns(2)
        # FULL
        with c1:
            st.markdown(f"**FULL — {emp}**")
            up_full = st.file_uploader("CSV/XLSX/XLS", type=["csv","xlsx","xls"], key=f"up_full_{emp}")
            if up_full is not None:
                state[emp]["FULL"]["name"]  = up_full.name
                # LINHA CORRIGIDA: Usa up_full em vez de up_file
                state[emp]["FULL"]["bytes"] = up_full.read() 
            
            if state[emp]["FULL"]["name"]:
                st.caption(f"FULL salvo: **{state[emp]['FULL']['name']}**")
            else:
                st.caption("Nenhum arquivo FULL salvo.")
                
        # Shopee/MT
        with c2:
            st.markdown(f"**Shopee/MT — {emp}**")
            up_v = st.file_uploader("CSV/XLSX/XLS", type=["csv","xlsx","xls"], key=f"up_v_{emp}")
            if up_v is not None:
                state[emp]["VENDAS"]["name"]  = up_v.name
                state[emp]["VENDAS"]["bytes"] = up_v.read()
            
            if state[emp]["VENDAS"]["name"]:
                st.caption(f"Vendas salvo: **{state[emp]['VENDAS']['name']}**")
            else:
                st.caption("Nenhum arquivo de Vendas salvo.")

        # Estoque Físico
        st.markdown("**Estoque Físico — (necessário para Compra Automática)**")
        up_e = st.file_uploader("CSV/XLSX/XLS", type=["csv","xlsx","xls"], key=f"up_e_{emp}")
        if up_e is not None:
            state[emp]["ESTOQUE"]["name"]  = up_e.name
            state[emp]["ESTOQUE"]["bytes"] = up_e.read()
        
        if state[emp]["ESTOQUE"]["name"]:
            st.caption(f"Estoque salvo: **{state[emp]['ESTOQUE']['name']}**")
        else:
            st.caption("Nenhum arquivo de Estoque salvo.")


        # Botões de Salvar e Limpar
        c3, c4 = st.columns([1,1])
        with c3:
            if st.button(f"Salvar {emp}", use_container_width=True, key=f"save_{emp}"):
                st.success(f"Status {emp} SALVO: FULL [{'OK' if state[emp]['FULL']['name'] else '–'}] • "
                           f"Shopee [{'OK' if state[emp]['VENDAS']['name'] else '–'}] • "
                           f"Estoque [{'OK' if state[emp]['ESTOQUE']['name'] else '–'}]")
        with c4:
            if st.button(f"Limpar {emp}", use_container_width=True, key=f"clr_{emp}"):
                state[emp] = {"FULL":{"name":None,"bytes":None},
                              "VENDAS":{"name":None,"bytes":None},
                              "ESTOQUE":{"name":None,"bytes":None}}
                st.info(f"{emp} limpo. (Recarregue a página para limpar o visual do upload.)")

        st.divider()

    bloco_empresa("ALIVVIA")
    bloco_empresa("JCA")