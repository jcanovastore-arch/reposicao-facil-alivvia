# mod_dados_empresas.py - MÓDULO DA TAB 1 - FIX V5.8
# SOLUÇÃO FINAL DE ESTABILIDADE: Replicando a lógica de persistência que funcionava no código antigo.

import streamlit as st
import logica_compra 

def render_tab1(state):
    """Renderiza toda a aba 'Dados das Empresas'."""
    st.subheader("Uploads fixos por empresa (os arquivos permanecem salvos após F5)")
    st.caption("O status azul confirma que o arquivo está salvo e persistirá após o F5.")

    def render_company_block_final(emp: str):
        st.markdown(f"### {emp}")
        
        # --- UPLOAD E STATUS ---
        def render_upload_slot(slot: str, label: str, col):
            saved_name = state[emp][slot]["name"]
            
            with col:
                st.markdown(f"**{label} — {emp}**")
                
                # 1. RENDERIZA O UPLOADER (Sempre)
                up_file = st.file_uploader("CSV/XLSX/XLS", type=["csv","xlsx","xls"], key=f"up_{slot}_{emp}")
                
                # 2. Ação: Se houver um novo upload (up_file is not None), salva os bytes
                if up_file is not None:
                    # Se for um novo arquivo ou diferente do salvo
                    if saved_name != up_file.name:
                        state[emp][slot]["name"] = up_file.name
                        state[emp][slot]["bytes"] = up_file.read()
                        st.success(f"Carregado: {up_file.name}") 
                
                # 3. Status Persistente: Mostra o status do arquivo SALVO na sessão (sobrevive ao F5)
                if state[emp][slot]["name"]:
                    # Este st.info/st.success permanece no F5, garantindo o feedback visual da persistência.
                    st.info(f"💾 **Salvo na Sessão**: {state[emp][slot]['name']}") 

        # Renderizar slots principais
        col_full, col_vendas = st.columns(2)
        render_upload_slot("FULL", "FULL", col_full)
        render_upload_slot("VENDAS", "Shopee/MT (Vendas)", col_vendas)

        # Renderizar Estoque
        st.markdown("---")
        col_estoque, _ = st.columns([1,1])
        render_upload_slot("ESTOQUE", "Estoque Físico", col_estoque)
        st.markdown("---")
        
        # --- Botões de Ação (Estrutura Antiga/Estável) ---
        c3, c4 = st.columns([1, 1])
        
        with c3:
            # O botão Salvar agora só confirma o status
            if st.button(f"Salvar {emp} (Confirmar)", use_container_width=True, key=f"save_{emp}", type="primary"):
                st.success(f"Status {emp} confirmado: Arquivos estão na sessão.")
        
        with c4:
            # Botão de Limpeza que dispara o rerun
            if st.button(f"Limpar {emp}", use_container_width=True, key=f"clr_{emp}", type="secondary"):
                state[emp] = {"FULL":{"name":None,"bytes":None},
                              "VENDAS":{"name":None,"bytes":None},
                              "ESTOQUE":{"name":None,"bytes":None}}
                st.info(f"{emp} limpo. Reinicie a página se necessário.")
                st.rerun() # Força a re-renderização para mostrar os uploaders limpos
        
        st.markdown("___") # Separador visual

    # Chamadas finais
    render_company_block_final("ALIVVIA")
    render_company_block_final("JCA")