# mod_dados_empresas.py - MÓDULO DA TAB 1 - FIX V6.5 (SOLUÇÃO FINAL DE ESTABILIDADE)
# ELIMINAÇÃO DO BOTÃO REPETITIVO 'Limpar TODOS' PARA RESOLVER O CRASH.
# A função de limpar tudo é movida para um botão único e global.

import streamlit as st
import logica_compra 

def render_company_block_final(state, emp: str):
    """Renderiza a seção de uploads e status para uma empresa, sem o botão 'Limpar TODOS'."""
    st.markdown(f"### {emp}")
    
    # --- UPLOAD E STATUS (USANDO CONDICIONAL PARA ESTABILIDADE) ---
    def render_upload_slot(slot: str, label: str, col):
        saved_name = state[emp][slot]["name"]
        
        with col:
            st.markdown(f"**{label} — {emp}**")
            
            if saved_name:
                # 1. ARQUIVO SALVO: Exibe o status e o botão Limpar Individual. (PERSISTÊNCIA GARANTIDA)
                
                st.info(f"💾 **Salvo na Sessão**: {saved_name}")
                
                # O botão Limpar INDIVIDUAL AGORA ESTÁ INTEGRADO E SEPARADO POR CHAVE ÚNICA.
                if st.button(f"🗑️ Limpar {label}", key=f"clr_{slot}_{emp}", use_container_width=True, type="secondary"):
                    state[emp][slot]["name"] = None
                    state[emp][slot]["bytes"] = None
                    st.rerun() # Dispara rerun para voltar ao estado de upload
                    
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
    st.markdown("---")
    st.markdown("___") # Separador visual

# --- FUNÇÃO PRINCIPAL DA TAB 1 ---
def render_tab1(state):
    """Renderiza toda a aba 'Dados das Empresas'."""
    st.subheader("Uploads fixos por empresa (os arquivos permanecem salvos após F5)")
    st.caption("O status azul abaixo confirma que o arquivo está salvo e persistirá após o F5.")

    # Renderiza ALIVVIA e JCA
    render_company_block_final(state, "ALIVVIA")
    render_company_block_final(state, "JCA")

    # --- BOTÃO GLOBAL DE LIMPEZA (ELIMINA O CONFLITO DE REPETIÇÃO) ---
    st.markdown("## ⚠️ Limpeza Global de Dados")
    st.warning("Este botão limpa TODOS os uploads de ALIVVIA e JCA salvos na sessão.")
    
    if st.button("🔴 Limpar TUDO (ALIVVIA e JCA)", key="clr_all_global", type="primary", use_container_width=True):
        
        # Limpa o estado de ambas as empresas
        for emp in ["ALIVVIA", "JCA"]:
            state[emp] = {"FULL":{"name":None,"bytes":None},
                          "VENDAS":{"name":None,"bytes":None},
                          "ESTOQUE":{"name":None,"bytes":None}}
        st.info("Todos os dados foram limpos. Reinicie a página se necessário.")
        st.rerun()