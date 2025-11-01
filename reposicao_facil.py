# reposicao_facil.py - VERSÃO DE INTERFACE V5.1 (ARQUITETURA FINAL)
# - Arquivo principal limpo.
# - Importa a lógica de negócios de 'logica_compra.py'.
# - Importa a renderização de UI de 'mod_dados_empresas.py', 'mod_compra_autom.py' e 'mod_alocacao.py'.

import datetime as dt
import pandas as pd
import streamlit as st

# =========================================================================
# MÓDULOS MODULARIZADOS
# =========================================================================
import logica_compra 
import mod_dados_empresas
import mod_compra_autom
import mod_alocacao # NOVO MÓDULO IMPORTADO

# Importando classes/funções necessárias do módulo de lógica e UI
from logica_compra import (
    baixar_xlsx_do_sheets,
    baixar_xlsx_por_link_google,
)

# MÓDULOS DE ORDEM DE COMPRA (SQLITE) - Mantendo a estrutura de import condicional
try:
    import ordem_compra 
    import gerenciador_oc 
except ImportError:
    pass 

VERSION = "v5.1 - ARQUITETURA FINAL"

# ===================== CONFIG BÁSICA =====================
st.set_page_config(page_title="Reposição Logística — Alivvia", layout="wide")

DEFAULT_SHEET_LINK = (
    "https://docs.google.com/sheets/d/1cTLARjq-B5g50dL6tcntg7lb_Iu0ta43/"
    "edit?usp=sharing&ouid=109458533144345974874&rtpof=true&sd=true"
)
DEFAULT_SHEET_ID = "1cTLARjq-B5g50dL6tcntg7lb_Iu0ta43"

# ===================== ESTADO =====================
def _ensure_state():
    st.session_state.setdefault("catalogo_df", None)
    st.session_state.setdefault("kits_df", None)
    st.session_state.setdefault("loaded_at", None)
    st.session_state.setdefault("alt_sheet_link", DEFAULT_SHEET_LINK)
    st.session_state.setdefault("oc_cesta", pd.DataFrame()) 
    st.session_state.setdefault("compra_autom_data", {})

    for emp in ["ALIVVIA", "JCA"]:
        st.session_state.setdefault(emp, {})
        st.session_state[emp].setdefault("FULL",   {"name": None, "bytes": None})
        st.session_state[emp].setdefault("VENDAS", {"name": None, "bytes": None})
        st.session_state[emp].setdefault("ESTOQUE",{"name": None, "bytes": None})

_ensure_state()

# ===================== UI: SIDEBAR =====================
with st.sidebar:
    st.subheader("Parâmetros")
    h  = st.selectbox("Horizonte (dias)", [30, 60, 90], index=1)
    g  = st.number_input("Crescimento % ao mês", value=0.0, step=1.0)
    LT = st.number_input("Lead time (dias)", value=0, step=1, min_value=0)

    st.markdown("---")
    st.subheader("Padrão (KITS/CAT) — Google Sheets")
    st.caption("Carrega **somente** quando você clicar. ID fixo da planilha foi deixado no código.")
    colA, colB = st.columns([1, 1])
    
    @st.cache_data(show_spinner="Baixando Planilha de Padrões KITS/CAT...")
    def get_padrao_from_sheets(sheet_id):
        return logica_compra._carregar_padrao_de_content(baixar_xlsx_do_sheets(sheet_id))

    with colA:
        if st.button("Carregar padrão agora", use_container_width=True):
            get_padrao_from_sheets.clear() 
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

    st.text_input("Link alternativo do Google Sheets (opcional)", key="alt_sheet_link",
                  help="Se necessário, cole o link e use o botão abaixo.")
    if st.button("Carregar deste link", use_container_width=True):
        try:
            content = baixar_xlsx_por_link_google(st.session_state.alt_sheet_link.strip())
            cat = logica_compra._carregar_padrao_de_content(content)
            st.session_state.catalogo_df = cat.catalogo_simples.rename(columns={"component_sku":"sku"})
            st.session_state.kits_df = cat.kits_reais
            st.session_state.loaded_at = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            st.success("Padrão carregado (link alternativo).")
        except Exception as e:
            st.session_state.catalogo_df = None; st.session_state.kits_df = None; st.session_state.loaded_at = None
            st.error(str(e))

# ===================== TÍTULO =====================
st.title("Reposição Logística — Alivvia")
if st.session_state.catalogo_df is None or st.session_state.kits_df is None:
    st.warning("► Carregue o **Padrão (KITS/CAT)** no sidebar antes de usar as abas.")

# ===================== ABAS =====================

tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📂 Dados das Empresas", 
    "🧮 Compra Automática", 
    "📦 Alocação de Compra", 
    "🛒 Ordem de Compra (OC)", 
    "✨ Gerenciador de OCs"
])

# ---------- RENDERIZAÇÃO POR MÓDULO ----------

with tab1:
    mod_dados_empresas.render_tab1(st.session_state)

with tab2:
    mod_compra_autom.render_tab2(st.session_state, h, g, LT)

with tab3:
    mod_alocacao.render_tab3(st.session_state)

# ---------- TAB 4: ORDEM DE COMPRA (OC) - CESTA DE ITENS (mantido) ----------
with tab4:
    if 'ordem_compra' in globals():
        st.subheader("🛒 Ordem de Compra (OC) - Cesta de Itens")
        
        cesta = st.session_state.oc_cesta
        if cesta.empty:
            st.info("A cesta de Ordem de Compra está vazia. Adicione itens da aba 'Compra Automática'.")
        else:
            st.success(f"Itens prontos para OC: {len(cesta)} itens de {len(cesta['fornecedor'].unique())} fornecedores.")
            st.dataframe(cesta, use_container_width=True)
            
            if st.button("Gerar e Finalizar Ordem de Compra (Módulo OC)", type="primary"):
                st.warning("Esta função requer a implementação do módulo `ordem_compra.py`.")
            
            if st.button("Limpar Cesta de OC", type="secondary"):
                st.session_state.oc_cesta = pd.DataFrame()
                st.rerun()

    else:
        st.error("ERRO: O módulo 'ordem_compra.py' não foi encontrado. As funcionalidades de OC não estão disponíveis.")

# ---------- TAB 5: GERENCIADOR DE OCS - CONTROLE DE RECEBIMENTO (mantido) ----------
with tab5:
    if 'gerenciador_oc' in globals():
        st.subheader("✨ Gerenciador de OCs - Controle de Recebimento")
        st.info("O Gerenciador de OCs está pronto para ser chamado a partir do módulo `gerenciador_oc.py`.")
        st.error("⚠️ ERRO CRÍTICO: Não foi possível autenticar com o Google Sheets. Configure 'credentials.json'.")
        st.warning("Isso é esperado no Streamlit Cloud, pois o arquivo 'credentials.json' de autenticação não está presente.")
    else:
        st.error("ERRO: O módulo 'gerenciador_oc.py' não foi encontrado. As funcionalidades de Gerenciamento de OC não estão disponíveis.")

st.caption("© Alivvia — simples, robusto e auditável. (V5.1)")