# mod_dados_empresas.py - MÓDULO DA TAB 1 - FIX V5.6
# SOLUÇÃO FINAL DE ESTABILIDADE: Remove o botão 'Limpar TODOS' para resolver a
# StreamlitAPIException. A persistência visual (F5) e o Clear Individual permanecem.

import streamlit as st
import logica_compra 

def render_tab1(state):
    """Renderiza toda a aba 'Dados das Empresas'."""
    st.subheader("Uploads fixos por empresa (os arquivos permanecem salvos após F5)")
    st.caption("O arquivo permanece salvo na sessão do servidor até você clicar em 'Limpar'.")

    def render_company_block_final(emp: str):
        st.markdown(f"### {emp}")
        
        # --- UPLOAD E STATUS ---
        def render_upload_slot(slot: str, label: str, col):
            saved_name = state[emp][slot]["name"]
            
            with col:
                st.markdown(f"**{label} — {emp}**")
                
                if saved_name:
                    # ARQUIVO SALVO (VERDE): Persiste no F5.
                    st.success(f"✅ Salvo: **{saved_name}**")
                else:
                    # ARQUIVO NÃO SALVO: Exibe o uploader.
                    up_file = st.file_uploader("CSV/XLSX/XLS", type=["csv","xlsx","xls"], key=f"up_{slot}_{emp}")
                    if up_file is not None:
                        state[emp][slot]["name"] = up_file.name
                        state[emp][slot]["bytes"] = up_file.read()
                        st.rerun()

        # Renderizar slots principais
        col_full, col_vendas = st.columns(2)
        render_upload_slot("FULL", "FULL", col_full)
        render_upload_slot("VENDAS", "Shopee/MT (Vendas)", col_vendas)

        # Renderizar Estoque
        st.markdown("---")
        col_estoque, _ = st.columns([1,1])
        render_upload_slot("ESTOQUE", "Estoque Físico", col_estoque)
        st.markdown("---")
        
        # --- BOTÕES DE LIMPEZA INDIVIDUAL (SOLUÇÃO DE CONFLITO) ---
        st.markdown("#### Ações de Limpeza de Arquivos")
        
        col_full_clr, col_vendas_clr, col_estoque_clr, _ = st.columns(4)
        
        clear_slots = [("FULL", "FULL", col_full_clr), 
                       ("VENDAS", "VENDAS", col_vendas_clr), 
                       ("ESTOQUE", "ESTOQUE", col_estoque_clr)]
        
        for slot, label, col in clear_slots:
            with col:
                if state[emp][slot]["name"]: 
                    if st.button(f"🗑️ Limpar {label}", key=f"clr_{slot}_{emp}", use_container_width=True, type="secondary"):
                        state[emp][slot]["name"] = None
                        state[emp][slot]["bytes"] = None
                        st.rerun() 
                else:
                    st.info(f"Slot {label} vazio.")

        # O BOTÃO 'Limpar TODOS' FOI REMOVIDO PARA ELIMINAR O CONFLITO DE REPETIÇÃO E GARANTIR A ESTABILIDADE.
        
        st.markdown("___") # Separador visual

    # Chamadas finais
    render_company_block_final("ALIVVIA")
    render_company_block_final("JCA")