# reposicao_facil.py - CÓDIGO FINAL DE ESTABILIDADE V8.2
# Integra módulos e corrige a lógica de persistência e a barra lateral.

import datetime as dt
import pandas as pd
import streamlit as st

# MÓDULOS MODULARIZADOS
import logica_compra 
import mod_compra_autom
import mod_alocacao 

# Importando funções e constantes do módulo de lógica
from logica_compra import (
    Catalogo,
    baixar_xlsx_do_sheets,
    baixar_xlsx_por_link_google,
    load_any_table_from_bytes, # ESSENCIAL
    mapear_tipo,               # ESSENCIAL
    mapear_colunas,            # ESSENCIAL
    calcular as calcular_compra,
    DEFAULT_SHEET_ID
)

# MÓDULOS DE ORDEM DE COMPRA (SQLITE) - Mantenha a estrutura
try:
    import ordem_compra 
    import gerenciador_oc 
except ImportError:
    pass 

VERSION = "v8.2 - RESTAURAÇÃO DE BOTÃO E ESTABILIDADE"

# ===================== CONFIG E ESTADO =====================
st.set_page_config(page_title="Reposição Logística — Alivvia", layout="wide")

DEFAULT_SHEET_LINK = "https://docs.google.com/spreadsheets/d/1cTLARjq-B5g50dL6tcntg7lb_Iu0ta43/edit?usp=sharing&ouid=109458533144345974874&rtpof=true&sd=true"

def _ensure_state():
    """Garante que todas as chaves de estado de sessão existam."""
    st.session_state.setdefault("catalogo_df", None)
    st.session_state.setdefault("kits_df", None)
    st.session_state.setdefault("loaded_at", None)
    st.session_state.setdefault("alt_sheet_link", DEFAULT_SHEET_LINK)
    st.session_state.setdefault("oc_cesta", pd.DataFrame()) # Adicionado cesta
    st.session_state.setdefault("compra_autom_data", {}) # Adicionado caching de compra
    
    for emp in ["ALIVVIA", "JCA"]:
        st.session_state.setdefault(emp, {})
        st.session_state[emp].setdefault("FULL",   {"name": None, "bytes": None})
        st.session_state[emp].setdefault("VENDAS", {"name": None, "bytes": None})
        st.session_state[emp].setdefault("ESTOQUE",{"name": None, "bytes": None})

_ensure_state()

# ===================== UI: SIDEBAR E PARÂMETROS =====================
with st.sidebar:
    st.subheader("Parâmetros")
    h  = st.selectbox("Horizonte (dias)", [30, 60, 90], index=1, key="h")
    g  = st.number_input("Crescimento % ao mês", value=0.0, step=1.0, key="g")
    LT = st.number_input("Lead time (dias)", value=0, step=1, min_value=0, key="LT")

    st.markdown("---")
    st.subheader("Padrão (KITS/CAT) — Google Sheets")
    st.caption("Carrega **somente** quando você clicar.")
    
    # Função para carregar o padrão (necessária para o cache)
    @st.cache_data(show_spinner="Baixando Planilha de Padrões KITS/CAT...")
    def get_padrao_from_sheets(sheet_id):
        # As funções de lógica precisam ser chamadas pelo logica_compra
        content = logica_compra.baixar_xlsx_do_sheets(sheet_id)
        return logica_compra._carregar_padrao_de_content(content)

    colA, colB = st.columns([1, 1])
    with colA:
        if st.button("Carregar padrão agora", use_container_width=True):
            try:
                # Chama a função de cache
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
            content = logica_compra.baixar_xlsx_por_link_google(st.session_state.alt_sheet_link.strip())
            cat = logica_compra._carregar_padrao_de_content(content)
            st.session_state.catalogo_df = cat.catalogo_simples.rename(columns={"component_sku":"sku"})
            st.session_state.kits_df = cat.kits_reais
            st.session_state.loaded_at = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            st.success("Padrão carregado (link alternativo).")
        except Exception as e:
            st.session_state.catalogo_df = None; st.session_state.kits_df = None; st.session_state.loaded_at = None
            st.error(str(e))
            
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

# ---------- TAB 1: UPLOADS (LÓGICA ESTÁVEL INTEGRADA) ----------
with tab1:
    st.subheader("Uploads fixos por empresa (os arquivos permanecem salvos após F5)")
    st.caption("O status azul abaixo confirma que o arquivo está salvo e persistirá após o F5.")

    def render_block(emp: str):
        st.markdown(f"### {emp}")
        
        def render_upload_slot(slot: str, label: str, col):
            saved_name = st.session_state[emp][slot]["name"]
            
            with col:
                st.markdown(f"**{label} — {emp}**")
                
                if saved_name:
                    # 1. ARQUIVO SALVO: Exibe o status e o botão Limpar Individual.
                    st.info(f"💾 **Salvo na Sessão**: {saved_name}")
                    
                    if st.button(f"🗑️ Limpar {label}", key=f"clr_{slot}_{emp}", use_container_width=True, type="secondary"):
                        st.session_state[emp][slot]["name"] = None
                        st.session_state[emp][slot]["bytes"] = None
                        st.rerun() 
                        
                else:
                    # 2. ARQUIVO NÃO SALVO: Exibe o uploader
                    up_file = st.file_uploader("CSV/XLSX/XLS", type=["csv","xlsx","xls"], key=f"up_{slot}_{emp}")
                    
                    if up_file is not None:
                        # Salva o arquivo e dispara rerun para mostrar o status persistente.
                        st.session_state[emp][slot]["name"] = up_file.name
                        st.session_state[emp][slot]["bytes"] = up_file.read()
                        st.rerun() 

        # Renderizar slots
        col_full, col_vendas = st.columns(2)
        render_upload_slot("FULL", "FULL", col_full)
        render_upload_slot("VENDAS", "Shopee/MT (Vendas)", col_vendas)

        st.markdown("---")
        col_estoque, _ = st.columns([1,1])
        render_upload_slot("ESTOQUE", "Estoque Físico", col_estoque)
        st.markdown("___") # Separador visual

    # Chamadas finais
    render_block("ALIVVIA")
    render_block("JCA")
    
    # Botão de Limpeza Global
    st.markdown("## ⚠️ Limpeza Total de Dados")
    if st.button("🔴 Limpar TUDO (ALIVVIA e JCA)", key="clr_all_global", type="primary", use_container_width=True):
        for emp in ["ALIVVIA", "JCA"]:
            st.session_state[emp] = {"FULL":{"name":None,"bytes":None},
                                     "VENDAS":{"name":None,"bytes":None},
                                     "ESTOQUE":{"name":None,"bytes":None}}
        st.info("Todos os dados foram limpos.")
        st.rerun()

# ---------- TAB 2: COMPRA AUTOMÁTICA ----------
with tab2:
    # O módulo mod_compra_autom.py agora usa as chaves h, g, LT salvas na sessão
    mod_compra_autom.render_tab2(st.session_state, st.session_state.h, st.session_state.g, st.session_state.LT)

# ---------- TAB 3: ALOCAÇÃO DE COMPRA ----------
with tab3:
    mod_alocacao.render_tab3(st.session_state)
    
# ... (Restante das Tabs 4 e 5)

st.caption("© Alivvia — simples, robusto e auditável. (V8.2)")