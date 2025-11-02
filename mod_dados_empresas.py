# mod_dados_empresas.py - MÓDULO DA TAB 1 - FIX V5.4.2
# FIX CRÍTICO: Isola o renderização dos botões 'Limpar' e 'Limpar TODOS' para resolver
# o StreamlitAPIException (Button Duplication) e garantir a estabilidade do F5.

import streamlit as st
import logica_compra 

def render_tab1(state):
    """Renderiza toda a aba 'Dados das Empresas'."""
    st.subheader("Uploads fixos por empresa (os arquivos permanecem salvos após F5)")
    st.caption("O arquivo permanece salvo na sessão do servidor até você clicar em 'Limpar'.")

    def bloco_empresa(emp: str):
        st.markdown(f"### {emp}")
        
        # Slots de dados e rótulos
        slots = [("FULL", "FULL"), ("VENDAS", "Shopee/MT (Vendas)")]
        
        # 1. BLOCO DE UPLOAD E STATUS (Sem botões de Limpar)
        
        # Renderiza FULL e VENDAS
        cols_upload = st.columns(2)
        for (slot, label), col in zip(slots, cols_upload):
            with col:
                st.markdown(f"**{label} — {emp}**")
                saved_name = state[emp][slot]["name"]

                if saved_name:
                    # Se SALVO: Exibe o status VERDE e oculta o uploader
                    st.success(f"✅ Salvo: **{saved_name}**")
                else:
                    # Se NÃO SALVO: Exibe o uploader
                    up_file = st.file_uploader("CSV/XLSX/XLS", type=["csv","xlsx","xls"], key=f"up_{slot}_{emp}")
                    if up_file is not None:
                        # Salva imediatamente e força rerun para mover para o estado VERDE.
                        state[emp][slot]["name"] = up_file.name
                        state[emp][slot]["bytes"] = up_file.read()
                        st.rerun() 
        
        # Renderiza ESTOQUE
        st.markdown("---")
        slot, label = "ESTOQUE", "Estoque Físico"
        col_estoque = st.columns([1])[0] # Coluna única para layout mais limpo
        with col_estoque:
            st.markdown(f"**{label} — {emp}**")
            saved_name = state[emp][slot]["name"]
            
            if saved_name:
                st.success(f"✅ Salvo: **{saved_name}**")
            else:
                up_file = st.file_uploader("CSV/XLSX/XLS", type=["csv","xlsx","xls"], key=f"up_{slot}_{emp}")
                if up_file is not None:
                    state[emp][slot]["name"] = up_file.name
                    state[emp][slot]["bytes"] = up_file.read()
                    st.rerun()


        # 2. BLOCO DE AÇÕES (Limpar)
        st.markdown("---")
        st.markdown("#### Ações de Limpeza de Arquivos")
        
        # Colunas dedicadas para os botões de limpeza individual (evita conflito)
        col_full, col_vendas, col_estoque_limpar = st.columns(3)
        
        # Botões de Limpeza Individual
        for col, slot, label in [(col_full, "FULL", "FULL"), 
                                 (col_vendas, "VENDAS", "VENDAS"), 
                                 (col_estoque_limpar, "ESTOQUE", "ESTOQUE")]:
            with col:
                # Se o arquivo estiver salvo, mostra o botão para limpar.
                if state[emp][slot]["name"]: 
                    if st.button(f"🗑️ Limpar {label}", key=f"clr_{slot}_{emp}", use_container_width=True, type="secondary"):
                        state[emp][slot]["name"] = None
                        state[emp][slot]["bytes"] = None
                        st.rerun() 
                else:
                    st.info(f"Slot {label} vazio.")


        # Botão Limpar TODOS (TOTALMENTE ISOLADO)
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