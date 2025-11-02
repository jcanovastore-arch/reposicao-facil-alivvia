# reposicao_facil.py - CÓDIGO FINAL DE ESTABILIDADE V7.2
# Integra módulos e inicializa a persistência real (LocalStorage)

import datetime as dt
import pandas as pd
import streamlit as st

# MÓDULOS MODULARIZADOS
import logica_compra 
import mod_dados_empresas
import mod_compra_autom
import mod_alocacao 

# MÓDULO DE PERSISTÊNCIA EXTERNA (LOCALSTORAGE)
try:
    from streamlit_ext import st_persistent_state
except ImportError:
    st_persistent_state = None 

# Importando funções e constantes do módulo de lógica
from logica_compra import (
    Catalogo,
    baixar_xlsx_do_sheets,
    baixar_xlsx_por_link_google,
    DEFAULT_SHEET_ID
)

# MÓDULOS DE ORDEM DE COMPRA (SQLITE) - Mantendo a estrutura
try:
    import ordem_compra 
    import gerenciador_oc 
except ImportError:
    pass 

VERSION = "v7.2 - ESTABILIDADE FINAL"

# ===================== CONFIG E ESTADO =====================
st.set_page_config(page_title="Reposição Logística — Alivvia", layout="wide")

DEFAULT_SHEET_LINK = "https://docs.google.com/spreadsheets/d/1cTLARjq-B5g50dL6tcntg7lb_Iu0ta43/edit?usp=sharing&ouid=109458533144345974874&rtpof=true&sd=true"

def _ensure_state():
    """Garante que todas as chaves de estado de sessão existam."""
    st.session_state.setdefault("catalogo_df", None)
    st.session_state.setdefault("kits_df", None)
    st.session_state.setdefault("loaded_at", None)
    st.session_state.setdefault("alt_sheet_link", DEFAULT_SHEET_LINK)
    
    # GARANTIA DE CHAVES DA EMPRESA
    for emp in ["ALIVVIA", "JCA"]:
        st.session_state.setdefault(emp, {})
        st.session_state[emp].setdefault("FULL",   {"name": None, "bytes": None})
        st.session_state[emp].setdefault("VENDAS", {"name": None, "bytes": None})
        st.session_state[emp].setdefault("ESTOQUE",{"name": None, "bytes": None})

_ensure_state()

# INICIALIZAÇÃO CRÍTICA DO LOCALSTORAGE (SOLUÇÃO DE PERSISTÊNCIA FINAL)
if st_persistent_state:
    st_persistent_state.initialize(
        keys=['ALIVVIA', 'JCA', 'catalogo_df', 'kits_df', 'h', 'g', 'LT', 'oc_cesta'] 
    )

# ===================== UI: SIDEBAR E PARÂMETROS =====================
with st.sidebar:
    st.subheader("Parâmetros")
    # Estas variáveis serão salvas no LocalStorage se a inicialização acima for bem-sucedida
    h  = st.selectbox("Horizonte (dias)", [30, 60, 90], index=1, key="h")
    g  = st.number_input("Crescimento % ao mês", value=0.0, step=1.0, key="g")
    LT = st.number_input("Lead time (dias)", value=0, step=1, min_value=0, key="LT")

    st.markdown("---")
    st.subheader("Padrão (KITS/CAT) — Google Sheets")
    # ... (Restante da lógica de carregamento do Google Sheets, usando st.session_state normalmente)
    
    @st.cache_data(show_spinner="Baixando Planilha de Padrões KITS/CAT...")
    def get_padrao_from_sheets(sheet_id):
        return logica_compra._carregar_padrao_de_content(baixar_xlsx_do_sheets(sheet_id))

    colA, colB = st.columns([1, 1])
    with colA:
        if st.button("Carregar padrão agora", use_container_width=True):
            try:
                cat = get_padrao_from_sheets(DEFAULT_SHEET_ID)
                st.session_state.catalogo_df = cat.catalogo_simples.rename(columns={"component_sku":"sku"})
                st.session_state.kits_df = cat.kits_reais
                st.session_state.loaded_at = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                st.success("Padrão carregado com sucesso.")
            except Exception as e:
                st.session_state.catalogo_df = None; st.session_state.kits_df = None; st.session_state.loaded_at = None
                st.error(str(e))
    with colB:
        st.link_button("🔗 Abrir no Drive (editar)", DEFAULT_SHEET_LINK, use_container_width=True)


# ===================== TÍTULO E ABAS =====================
st.title("Reposição Logística — Alivvia")
if st.session_state.catalogo_df is None or st.session_state.kits_df is None:
    st.warning("► Carregue o **Padrão (KITS/CAT)** no sidebar antes de usar as abas.")

tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📂 Dados das Empresas", 
    "🧮 Compra Automática", 
    "📦 Alocação de Compra", 
    "🛒 Ordem de Compra (OC)", 
    "✨ Gerenciador de OCs"
])

# ---------- RENDERIZAÇÃO MODULARIZADA (ESTABILIZADA) ----------

# Chamadas com a correção de argumento (passando st.session_state)
with tab1:
    mod_dados_empresas.render_tab1(st.session_state)

with tab2:
    mod_compra_autom.render_tab2(st.session_state, st.session_state.h, st.session_state.g, st.session_state.LT)

with tab3:
    mod_alocacao.render_tab3(st.session_state)
    
# ... (Restante das Tabs 4 e 5)