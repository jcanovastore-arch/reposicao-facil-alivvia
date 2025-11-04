# mod_alocacao.py - TAB 3 - V10.7 (Desativado)
# O novo fluxo da Tab 2 (Conjunta) torna esta aba desnecessária.

import streamlit as st

def render_tab3(state):
    """Renderiza a Tab 3 (Alocação de Compra)"""
    st.subheader("Alocação de Compra (Desativado)")
    st.info(
        "Este fluxo foi desativado conforme a nova lógica (V10.7).\n\n"
        "Use a **Tab 2 (Compra Automática)** e selecione a opção **'CONJUNTA'** "
        "para ver os cálculos de ALIVVIA e JCA separadamente."
    )