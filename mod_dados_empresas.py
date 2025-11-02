# mod_dados_empresas.py - MÓDULO DA TAB 1 - FIX V5.2
# FIX CRÍTICO: Implementa feedback visual (verde) e lógica de esconder/mostrar o uploader
# para garantir que o status de "salvo" seja claro e persistente.

import streamlit as st
import logica_compra # Importa o módulo de lógica para acessar o read()

def render_tab1(state):
    """Renderiza toda a aba 'Dados das Empresas'."""
    st.subheader("Uploads fixos por empresa (os arquivos permanecem salvos após F5)")
    st.caption("Faça o upload. O arquivo será salvo na sessão até você clicar em 'Limpar'.")

    def bloco_empresa(emp: str):
        st.markdown(f"### {emp}")
        
        def render_slot(slot: str, label: str):
            """Função unificada para renderizar o slot de upload/status."""
            
            saved_name = state[emp][slot]["name"]
            
            if saved_name:
                # 1. ARQUIVO SALVO: Exibe o status em VERDE e o botão Limpar.
                st.success(f"✅ {label} salvo: **{saved_name}**")
                
                # Botão "Limpar" para remover o arquivo da sessão
                if st.button(f"🗑️ Limpar {label}", key=f"clr_{slot}_{emp}", use_container_width=True, type="secondary"):
                    state[emp][slot]["name"] = None
                    state[emp][slot]["bytes"] = None
                    st.rerun() # Força a re-renderização para mostrar o uploader
            else:
                # 2. ARQUIVO NÃO SALVO: Exibe o uploader.
                up_file = st.file_uploader(f"👆 {label} — {emp} (CSV/XLSX/XLS)", 
                                           type=["csv","xlsx","xls"], key=f"up_{slot}_{emp}")
                
                if up_file is not None:
                    # Se um arquivo é carregado, salva imediatamente e força rerun para mostrar o status verde.
                    state[emp][slot]["name"] = up_file.name
                    state[emp][slot]["bytes"] = up_file.read()
                    st.rerun() # RERUN CRÍTICO: Fixa o estado antes que o widget resete.
        
        # Estrutura de colunas para FULL e VENDAS
        c1, c2 = st.columns(2)
        with c1:
            st.markdown(f"**FULL — {emp}**")
            render_slot("FULL", "FULL")
        
        with c2:
            st.markdown(f"**Shopee/MT — {emp}**")
            render_slot("VENDAS", "Shopee/MT (Vendas)")

        # Estoque Físico
        st.markdown("**Estoque Físico — (necessário para Compra Automática)**")
        render_slot("ESTOQUE", "Estoque Físico")
        
        st.divider()

    bloco_empresa("ALIVVIA")
    bloco_empresa("JCA")