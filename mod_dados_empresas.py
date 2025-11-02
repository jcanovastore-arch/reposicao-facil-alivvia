# mod_dados_empresas.py - MÓDULO DA TAB 1 - FIX V5.4.3
# FIX CRÍTICO: Isola os botões de Limpeza (que causam StreamlitAPIException)
# em funções e blocos separados para garantir a estabilidade total no F5.

import streamlit as st
import logica_compra 

# Função que renderiza os uploads e o status verde
def render_upload_section(state, emp: str):
    st.markdown(f"### {emp}")
    
    def render_slot(slot: str, label: str, col):
        saved_name = state[emp][slot]["name"]
        
        with col:
            st.markdown(f"**{label} — {emp}**")
            
            if saved_name:
                # 1. ARQUIVO SALVO (VERDE)
                st.success(f"✅ Salvo: **{saved_name}**")
            else:
                # 2. ARQUIVO NÃO SALVO: Exibe o uploader.
                up_file = st.file_uploader("CSV/XLSX/XLS", type=["csv","xlsx","xls"], key=f"up_{slot}_{emp}")
                
                if up_file is not None:
                    # Salva imediatamente e força rerun para mover para o estado VERDE.
                    state[emp][slot]["name"] = up_file.name
                    state[emp][slot]["bytes"] = up_file.read()
                    st.rerun() 

    # Colunas de Upload
    c1, c2 = st.columns(2)
    render_slot("FULL", "FULL", c1)
    render_slot("VENDAS", "Shopee/MT (Vendas)", c2)

    # Estoque Físico
    st.markdown("---")
    col_estoque, _ = st.columns([1,1])
    render_slot("ESTOQUE", "Estoque Físico", col_estoque)
    st.markdown("---")

# Função que renderiza os botões de limpeza (separadamente para evitar conflito)
def render_clear_buttons(state, emp: str):
    
    # 1. Botões de Limpeza Individual
    st.markdown(f"#### Ações de Limpeza de Arquivos {emp}")
    col_full, col_vendas, col_estoque_limpar = st.columns(3)
    
    for col, slot, label in [(col_full, "FULL", "FULL"), 
                             (col_vendas, "VENDAS", "VENDAS"), 
                             (col_estoque_limpar, "ESTOQUE", "ESTOQUE")]:
        with col:
            if state[emp][slot]["name"]: 
                if st.button(f"🗑️ Limpar {label}", key=f"clr_{slot}_{emp}", use_container_width=True, type="secondary"):
                    state[emp][slot]["name"] = None
                    state[emp][slot]["bytes"] = None
                    st.rerun() 
            else:
                st.info(f"Slot {label} vazio.")

    # 2. Botão Limpar TODOS (TOTALMENTE ISOLADO POR COMPANHIA)
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
    st.markdown("___") # Separador para JCA


def render_tab1(state):
    """Renderiza toda a aba 'Dados das Empresas'."""
    st.subheader("Uploads fixos por empresa (os arquivos permanecem salvos após F5)")
    st.caption("O arquivo permanece salvo na sessão do servidor até você clicar em 'Limpar'.")

    # ALIVVIA
    render_upload_section(state, "ALIVVIA")
    render_clear_buttons(state, "ALIVVIA")
    
    # JCA
    render_upload_section(state, "JCA")
    render_clear_buttons(state, "JCA")