# mod_dados_empresas.py - MÓDULO DA TAB 1 - FIX V5.7
# SOLUÇÃO DEFINITIVA: Reversão para a lógica de persistência "simples e funcional" do código antigo, 
# garantindo que o status de "salvo" seja visualmente persistente após o F5.

import streamlit as st
import logica_compra 

def render_tab1(state):
    """Renderiza toda a aba 'Dados das Empresas'."""
    st.subheader("Uploads fixos por empresa (os arquivos permanecem salvos após F5)")
    st.caption("O arquivo permanece salvo na sessão do servidor. O status azul confirma a persistência.")

    def render_company_block_final(emp: str):
        st.markdown(f"### {emp}")
        
        def render_upload_slot(slot: str, label: str, col):
            saved_name = state[emp][slot]["name"]
            
            with col:
                st.markdown(f"**{label} — {emp}**")
                
                # 1. RENDERIZA O UPLOADER SEMPRE (Mesmo que ele resete no F5)
                up_file = st.file_uploader("CSV/XLSX/XLS", type=["csv","xlsx","xls"], key=f"up_{slot}_{emp}")
                
                # 2. Ação: Se houver um novo upload (up_file is not None), salva os bytes
                if up_file is not None:
                    state[emp][slot]["name"] = up_file.name
                    state[emp][slot]["bytes"] = up_file.read()
                    # Feedback verde temporário
                    st.success(f"Carregado: {up_file.name}") 
                
                # 3. Status Persistente: A chave da correção é mostrar o status baseado no session_state
                if saved_name:
                    # Este st.info/st.success permanece no F5, garantindo o feedback visual.
                    st.info(f"💾 **Salvo na Sessão**: {saved_name}") 

        # --- Renderizar Blocos ---
        col_full, col_vendas = st.columns(2)
        render_upload_slot("FULL", "FULL", col_full)
        render_upload_slot("VENDAS", "Shopee/MT (Vendas)", col_vendas)

        st.markdown("---")
        col_estoque, _ = st.columns([1,1])
        render_upload_slot("ESTOQUE", "Estoque Físico", col_estoque)
        st.markdown("---")
        
        # --- Botões de Limpeza Individual ---
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
        st.markdown("---")
        st.markdown("___") # Separador visual

    # Chamadas finais
    render_company_block_final("ALIVVIA")
    render_company_block_final("JCA")