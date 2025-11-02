# mod_dados_empresas.py - MÓDULO DA TAB 1 - FIX V7.2 (ESTABILIDADE MÁXIMA E PERSISTÊNCIA)
# Resolve o TypeError e garante que a persistência condicional (LocalStorage) seja estável.

import streamlit as st
import logica_compra 

def render_tab1(state):
    """Renderiza toda a aba 'Dados das Empresas'."""
    st.subheader("Uploads fixos por empresa (os arquivos permanecem salvos após F5)")
    st.caption("O status azul abaixo confirma que o arquivo está salvo e persistirá após o F5. Use o botão Limpar para remover um arquivo individualmente.")

    def render_block(emp: str):
        st.markdown(f"### {emp}")
        
        # --- UPLOAD E STATUS (ESTABILIDADE GARANTIDA) ---
        def render_upload_slot(slot: str, label: str, col):
            saved_name = state[emp][slot]["name"]
            
            with col:
                st.markdown(f"**{label} — {emp}**")
                
                if saved_name:
                    # 1. ARQUIVO SALVO: Exibe o status e o botão Limpar Individual.
                    st.info(f"💾 **Salvo na Sessão**: {saved_name}")
                    
                    if st.button(f"🗑️ Limpar {label}", key=f"clr_{slot}_{emp}", use_container_width=True, type="secondary"):
                        state[emp][slot]["name"] = None
                        state[emp][slot]["bytes"] = None
                        st.rerun() 
                        
                else:
                    # 2. ARQUIVO NÃO SALVO: Exibe o uploader
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
        st.markdown("___") # Separador visual

    # Chamadas finais
    render_block("ALIVVIA")
    render_block("JCA")
    
    # --- BOTÃO GLOBAL ÚNICO (Limpeza Final) ---
    st.markdown("## ⚠️ Limpeza Total de Dados")
    st.warning("Este botão limpa TODOS os uploads de ALIVVIA e JCA salvos na sessão.")
    
    if st.button("🔴 Limpar TUDO (ALIVVIA e JCA)", key="clr_all_global", type="primary", use_container_width=True):
        
        for emp in ["ALIVVIA", "JCA"]:
            state[emp] = {"FULL":{"name":None,"bytes":None},
                          "VENDAS":{"name":None,"bytes":None},
                          "ESTOQUE":{"name":None,"bytes":None}}
        st.info("Todos os dados foram limpos.")
        st.rerun()