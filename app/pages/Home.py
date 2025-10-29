import streamlit as st
from app.core.config import VERSION

def main():
    st.set_page_config(page_title="Alivvia — Gestão", layout="wide")
    st.title("Alivvia — Gestão")
    st.caption(f"Versão {VERSION}")
    st.markdown("Selecione a página no menu lateral (ou mantenha seu fluxo atual).")

if __name__ == "__main__":
    main()
