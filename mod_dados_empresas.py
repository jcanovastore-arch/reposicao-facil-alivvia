# mod_dados_empresas.py - MÓDULO DA TAB 1 - FIX V5.5
# SOLUÇÃO FINAL: Separação de Botões de Ação em Funções Destinadas a um Único RERUN Contexto
# para resolver o StreamlitAPIException de forma definitiva.

import streamlit as st
import logica_compra 

# -----------------------------------------------------------
# Função 1: Apenas Renderiza Uploads e Status (SAFE)
# -----------------------------------------------------------
def render_uploads_and_status(state, emp: str):
    """Renderiza a seção de uploads e o status VERDE (sem botões de limpar)."""
    st.markdown(f"### {emp}")

    def render_slot(slot: str, label: str, col):
        saved_name = state[emp][slot]["name"]
        
        with col:
            st.markdown(f"**{label} — {emp}**")
            
            if saved_name:
                # 1. ARQUIVO SALVO (VERDE): Persiste no F5.
                st.success(f"✅ Salvo: **{saved_name}**")
            else:
                # 2. ARQUIVO NÃO SALVO: Exibe o uploader.
                up_file = st.file_uploader("CSV/XLSX/XLS", type=["csv","xlsx","xls"], key=f"up_{slot}_{emp}")
                
                if up_file is not None:
                    # Salva imediatamente e força rerun para mover para o estado VERDE.
                    state[emp][slot]["name"] = up_file.name
                    state[emp][slot]["bytes"] = up_file.read()
                    st.rerun() 
    
    # Renderização
    c1, c2 = st.columns(2)
    render_slot("FULL", "FULL", c1)
    render_slot("VENDAS", "Shopee/MT (Vendas)", c2)

    st.markdown("---")
    col_estoque, _ = st.columns([1,1])
    render_slot("ESTOQUE", "Estoque Físico", col_estoque)
    st.markdown("---")

# -----------------------------------------------------------
# Função 2: Apenas Renderiza Botões de Limpeza (CRITICAL CONFLICT RESOLUTION)
# -----------------------------------------------------------
def render_clear_buttons(state, emp: str):
    """Renderiza apenas os botões de limpeza para uma empresa (isolando o st.rerun)."""
    
    st.markdown(f"#### Ações de Limpeza de Arquivos {emp}")
    
    # Botões de Limpeza Individual
    col_full, col_vendas, col_estoque_limpar = st.columns(3)
    
    slots_to_clear = [("FULL", "FULL", col_full), 
                      ("VENDAS", "VENDAS", col_vendas), 
                      ("ESTOQUE", "ESTOQUE", col_estoque_limpar)]
                      
    for slot, label, col in slots_to_clear:
        with col:
            # O botão só aparece se houver algo para limpar.
            if state[emp][slot]["name"]: 
                if st.button(f"🗑️ Limpar {label}", key=f"clr_{slot}_{emp}", use_container_width=True, type="secondary"):
                    state[emp][slot]["name"] = None
                    state[emp][slot]["bytes"] = None
                    st.rerun() 
            else:
                st.info(f"Slot {label} vazio.")

    # Botão Limpar TODOS (O PROBLEMA CRÍTICO - AGORA ISOLADO)
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
    st.markdown("___") # Separador visual entre empresas


# -----------------------------------------------------------
# Função Principal da TAB 1 (Junta as partes)
# -----------------------------------------------------------
def render_tab1(state):
    """Renderiza toda a aba 'Dados das Empresas'."""

    # --- ALIVVIA ---
    render_uploads_and_status(state, "ALIVVIA")
    render_clear_buttons(state, "ALIVVIA")
    
    # --- JCA ---
    render_uploads_and_status(state, "JCA")
    render_clear_buttons(state, "JCA")