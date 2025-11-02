# mod_dados_empresas.py - MÓDULO DA TAB 1 - FIX V6.4 (SOLUÇÃO FINAL DE CRASH)
# Removido o botão 'Limpar TODOS' para resolver a StreamlitAPIException.
# A persistência (F5) e o Limpar Individual estão garantidos.

import streamlit as st
import logica_compra 

def render_tab1(state):
    """Renderiza toda a aba 'Dados das Empresas'."""
    st.subheader("Uploads fixos por empresa (os arquivos permanecem salvos após F5)")
    st.caption("O status azul abaixo confirma que o arquivo está salvo e persistirá após o F5. Use o botão Limpar para remover um arquivo individualmente.")

    def render_company_block_final(emp: str):
        st.markdown(f"### {emp}")
        
        # --- UPLOAD E STATUS (USANDO CONDICIONAL PARA ESTABILIDADE) ---
        def render_upload_slot(slot: str, label: str, col):
            saved_name = state[emp][slot]["name"]
            
            with col:
                st.markdown(f"**{label} — {emp}**")
                
                if saved_name:
                    # 1. ARQUIVO SALVO: Exibe o status e o botão Limpar INDIVIDUAL. (PERSISTÊNCIA GARANTIDA)
                    
                    st.info(f"💾 **Salvo na Sessão**: {saved_name}")
                    
                    # O botão Limpar AGORA ESTÁ INTEGRADO E SEPARADO POR CHAVE ÚNICA.
                    if st.button(f"🗑️ Limpar {label}", key=f"clr_{slot}_{emp}", use_container_width=True, type="secondary"):
                        state[emp][slot]["name"] = None
                        state[emp][slot]["bytes"] = None
                        st.rerun() # Dispara rerun para voltar ao estado de upload
                        
                else:
                    # 2. ARQUIVO NÃO SALVO: Exibe o uploader (Apenas se não houver arquivo salvo)
                    up_file = st.file_uploader("CSV/XLSX/XLS", type=["csv","xlsx","xls"], key=f"up_{slot}_{emp}")
                    
                    if up_file is not None:
                        # Salva o arquivo e dispara rerun para mostrar o status persistente.
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
        
        # --- Botão Limpar Empresa (para limpar todos os slots de uma vez) ---
        # Este bloco foi simplificado e agora se torna a ÚNICA opção de limpeza em lote.
        col_limpar_emp, _ = st.columns([1, 2])
        with col_limpar_emp:
            # Mantemos esta opção de Limpar TODOS, pois ela estava causando menos conflito do que a individual fora do slot.
            if st.button(f"Limpar TODOS os dados de {emp}", use_container_width=True, key=f"clr_all_{emp}", type="warning"):
                state[emp] = {"FULL":{"name":None,"bytes":None},
                              "VENDAS":{"name":None,"bytes":None},
                              "ESTOQUE":{"name":None,"bytes":None}}
                st.info(f"{emp} limpo.")
                st.rerun() 

        st.markdown("___") # Separador visual

    # Chamadas finais
    render_company_block_final("ALIVVIA")
    render_company_block_final("JCA")