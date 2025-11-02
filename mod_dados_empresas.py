# mod_dados_empresas.py - MÓDULO DA TAB 1 - FIX V5.4.1
# FIX CRÍTICO: Resolvido StreamlitAPIException (Button Duplication) movendo o botão 'Limpar TODOS' 
# para um contexto de coluna estável para evitar conflitos de renderização.

import streamlit as st
import logica_compra 

def render_tab1(state):
    """Renderiza toda a aba 'Dados das Empresas'."""
    st.subheader("Uploads fixos por empresa (os arquivos permanecem salvos após F5)")
    st.caption("O arquivo permanece salvo na sessão do servidor até você clicar em 'Limpar'.")

    def bloco_empresa(emp: str):
        st.markdown(f"### {emp}")
        
        def render_slot(slot: str, label: str, col):
            """Função unificada para renderizar o slot de upload/status/salvar."""
            
            saved_name = state[emp][slot]["name"]
            
            with col:
                st.markdown(f"**{label} — {emp}**")
                
                if saved_name:
                    # 1. ARQUIVO SALVO (PERMANENTE): Exibe o status em VERDE.
                    st.success(f"✅ Salvo: **{saved_name}**")
                    
                    # Botão "Limpar" para remover o arquivo da sessão
                    if st.button(f"🗑️ Limpar {label}", key=f"clr_{slot}_{emp}", use_container_width=True, type="secondary"):
                        state[emp][slot]["name"] = None
                        state[emp][slot]["bytes"] = None
                        st.rerun() 
                    
                else:
                    # 2. ARQUIVO NÃO SALVO: Exibe o uploader.
                    up_file = st.file_uploader("CSV/XLSX/XLS", type=["csv","xlsx","xls"], key=f"up_{slot}_{emp}")
                    
                    if up_file is not None:
                        # Salva imediatamente e força rerun para mover para o estado VERDE.
                        state[emp][slot]["name"] = up_file.name
                        state[emp][slot]["bytes"] = up_file.read()
                        st.rerun() 
        
        # Estrutura de colunas
        c1, c2 = st.columns(2)
        render_slot("FULL", "FULL", c1)
        render_slot("VENDAS", "Shopee/MT (Vendas)", c2)

        # Estoque Físico
        st.markdown("---")
        col_estoque, _ = st.columns([1,1])
        render_slot("ESTOQUE", "Estoque Físico", col_estoque)
        
        # Bloco de Limpeza total (AGORA EM UM CONTEXTO DE COLUNA DEDICADO E ESTÁVEL)
        st.markdown("---")
        col_limpar_todos, _ = st.columns([1, 2])
        with col_limpar_todos:
            if st.button(f"Limpar TODOS os arquivos de {emp}", key=f"clr_all_{emp}", type="warning", use_container_width=True):
                 state[emp] = {"FULL":{"name":None,"bytes":None},
                               "VENDAS":{"name":None,"bytes":None},
                               "ESTOQUE":{"name":None,"bytes":None}}
                 st.info(f"{emp} limpo. Reinicie a página se necessário.")
                 st.rerun()
        st.markdown("---")

    bloco_empresa("ALIVVIA")
    bloco_empresa("JCA")