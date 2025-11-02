# mod_dados_empresas.py - MÓDULO DA TAB 1 - FIX V6.6 (FLUXO DE PERSISTÊNCIA OTIMIZADO)
# Otimização do fluxo de upload para garantir que o save no session_state ocorra ANTES 
# do rerun, resolvendo o problema de F5.

import streamlit as st
import logica_compra 

def render_tab1(state):
    """Renderiza toda a aba 'Dados das Empresas'."""
    st.subheader("Uploads fixos por empresa (os arquivos permanecem salvos após F5)")
    st.caption("O status azul abaixo confirma que o arquivo está salvo e persistirá após o F5. Use o botão Limpar para remover um arquivo individualmente.")

    def render_company_block_final(emp: str):
        st.markdown(f"### {emp}")
        
        # --- UPLOAD E STATUS (FLUXO OTIMIZADO) ---
        def render_upload_slot(slot: str, label: str, col):
            saved_name = state[emp][slot]["name"]
            
            with col:
                st.markdown(f"**{label} — {emp}**")
                
                if saved_name:
                    # 1. ARQUIVO SALVO: Exibe o status e o botão Limpar Individual. (ESTADO PERSISTENTE)
                    
                    st.info(f"💾 **Salvo na Sessão**: {saved_name}")
                    
                    if st.button(f"🗑️ Limpar {label}", key=f"clr_{slot}_{emp}", use_container_width=True, type="secondary"):
                        state[emp][slot]["name"] = None
                        state[emp][slot]["bytes"] = None
                        st.rerun() 
                        
                else:
                    # 2. ARQUIVO NÃO SALVO: Exibe o uploader
                    up_file = st.file_uploader("CSV/XLSX/XLS", type=["csv","xlsx","xls"], key=f"up_{slot}_{emp}")
                    
                    if up_file is not None:
                        # LEITURA E SALVAMENTO CRÍTICO: Garantir que os bytes sejam lidos ANTES do rerun
                        try:
                            file_bytes = up_file.read()
                            if file_bytes:
                                state[emp][slot]["name"] = up_file.name
                                state[emp][slot]["bytes"] = file_bytes
                                st.rerun() # Dispara rerun para entrar no estado 'saved_name'
                        except Exception as e:
                            st.error(f"Falha ao ler o arquivo: {e}") 

        # Renderizar slots principais
        col_full, col_vendas = st.columns(2)
        render_upload_slot("FULL", "FULL", col_full)
        render_upload_slot("VENDAS", "Shopee/MT (Vendas)", col_vendas)

        # Renderizar Estoque
        st.markdown("---")
        col_estoque, _ = st.columns([1,1])
        render_upload_slot("ESTOQUE", "Estoque Físico", col_estoque)
        st.markdown("---")
        
        # --- Botão Limpar Empresa (Para limpar todos os slots de uma vez) ---
        col_limpar_emp, _ = st.columns([1, 2])
        with col_limpar_emp:
            if st.button(f"Limpar TODOS os dados de {emp}", use_container_width=True, key=f"clr_all_{emp}", type="warning"):
                state[emp] = {"FULL":{"name":None,"bytes":None},
                              "VENDAS":{"name":None,"bytes":None},
                              "ESTOQUE":{"name":None,"bytes":None}}
                st.info(f"{emp} limpo.")
                st.rerun() 

        st.markdown("___") # Separador visual

    # Chamadas finais
    render_company_block_final(state, "ALIVVIA")
    render_company_block_final(state, "JCA")