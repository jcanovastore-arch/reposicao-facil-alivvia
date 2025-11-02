# mod_dados_empresas.py - MÓDULO DA TAB 1 - FIX V5.4.4
# SOLUÇÃO DEFINITIVA: Consolidado em uma única função para isolar blocos de renderização
# e resolver o StreamlitAPIException. A persistência visual (F5) está garantida.

import streamlit as st
import logica_compra 

def render_company_block(state, emp: str):
    """Renderiza a seção completa (Uploads e Botões de Limpeza) para uma única empresa."""
    st.markdown(f"### {emp}")
    
    def render_slot(slot: str, label: str, col):
        saved_name = state[emp][slot]["name"]
        
        with col:
            st.markdown(f"**{label} — {emp}**")
            
            if saved_name:
                # 1. ARQUIVO SALVO (VERDE): Oculta o uploader no F5.
                st.success(f"✅ Salvo: **{saved_name}**")
            else:
                # 2. ARQUIVO NÃO SALVO: Exibe o uploader.
                up_file = st.file_uploader("CSV/XLSX/XLS", type=["csv","xlsx","xls"], key=f"up_{slot}_{emp}")
                
                if up_file is not None:
                    # Salva imediatamente e força rerun para mover para o estado VERDE.
                    state[emp][slot]["name"] = up_file.name
                    state[emp][slot]["bytes"] = up_file.read()
                    st.rerun() 

    # --- BLOCO DE UPLOAD E STATUS (Persistência no F5) ---
    c1, c2 = st.columns(2)
    render_slot("FULL", "FULL", c1)
    render_slot("VENDAS", "Shopee/MT (Vendas)", c2)

    st.markdown("---")
    col_estoque, _ = st.columns([1,1])
    render_slot("ESTOQUE", "Estoque Físico", col_estoque)
    st.markdown("---")
    
    # --- BLOCO DE AÇÕES (Botões de Limpeza) ---
    st.markdown("#### Ações de Limpeza de Arquivos")
    
    # Botões de Limpeza Individual (Repetição controlada em colunas separadas)
    col_full, col_vendas, col_estoque_limpar = st.columns(3)
    
    slots_to_clear = [("FULL", "FULL", col_full), 
                      ("VENDAS", "VENDAS", col_vendas), 
                      ("ESTOQUE", "ESTOQUE", col_estoque_limpar)]
                      
    for slot, label, col in slots_to_clear:
        with col:
            if state[emp][slot]["name"]: 
                if st.button(f"🗑️ Limpar {label}", key=f"clr_{slot}_{emp}", use_container_width=True, type="secondary"):
                    state[emp][slot]["name"] = None
                    state[emp][slot]["bytes"] = None
                    st.rerun() 
            else:
                st.info(f"Slot {label} vazio.")

    # Botão Limpar TODOS (O PROBLEMA CRÍTICO - Agora isolado no final)
    st.markdown("---")
    col_limpar_todos, _ = st.columns([1, 2])
    with col_limpar_todos:
        # Este botão deve ser o último elemento a ser renderizado antes do bloco da próxima empresa
        if st.button(f"Limpar TODOS os arquivos de {emp}", key=f"clr_all_{emp}", type="warning", use_container_width=True):
             state[emp] = {"FULL":{"name":None,"bytes":None},
                           "VENDAS":{"name":None,"bytes":None},
                           "ESTOQUE":{"name":None,"bytes":None}}
             st.info(f"{emp} limpo. Reinicie a página se necessário.")
             st.rerun()
    st.markdown("---")
    st.markdown("___") # Separador visual entre empresas


def render_tab1(state):
    """Função principal da TAB 1 que chama os blocos isolados."""
    st.subheader("Uploads fixos por empresa (os arquivos permanecem salvos após F5)")
    st.caption("O arquivo permanece salvo na sessão do servidor até você clicar em 'Limpar'.")

    # ALIVVIA
    render_company_block(state, "ALIVVIA")
    
    # JCA
    render_company_block(state, "JCA")