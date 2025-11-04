# gerenciador_oc.py - Módulo de Gestão de OCs (V10.15)
# - FIX: (V10.15) Renomeia 'render_tab5' para 'display_gerenciador_interface'
# - FIX: (V10.15) Corrige 'KeyError' de colunas duplicadas no merge
# - Mantém (V10.8) Lógica de DB, Edição de Status, e Impressão

import json
import datetime as dt
import os
import sqlite3
from typing import Dict, List, Any

import streamlit as st
import pandas as pd
import numpy as np

# Importa as funções de DB e HTML do módulo de OC
try:
    from ordem_compra import _get_db_connection, gerar_html_oc, DB_FILE
    from ordem_compra import STATUS_PENDENTE, STATUS_BAIXADA, STATUS_CANCELADA
except ImportError:
    st.error("Falha ao importar 'ordem_compra.py'. Arquivo ausente.")
    st.stop()

# --- LÓGICA DE CARREGAMENTO DO DB ---
@st.cache_data(ttl=300) # Cache de 5 minutos
def load_ocs_from_db(filtro_empresa: List[str], filtro_status: List[str]) -> pd.DataFrame:
    """Carrega as OCs do banco de dados com base nos filtros."""
    try:
        conn = _get_db_connection()
        
        query = "SELECT OC_ID, EMPRESA, FORNECEDOR, DATA_OC, DATA_PREVISTA, VALOR_TOTAL_R, STATUS, ITENS_COUNT FROM ordens_compra"
        conditions = []
        params = []
        
        if filtro_empresa:
            conditions.append(f"EMPRESA IN ({','.join('?'*len(filtro_empresa))})")
            params.extend(filtro_empresa)
        
        if filtro_status:
            conditions.append(f"STATUS IN ({','.join('?'*len(filtro_status))})")
            params.extend(filtro_status)
            
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
            
        query += " ORDER BY DATA_OC DESC"
        
        df = pd.read_sql_query(query, conn, params=params)
        return df
    except Exception as e:
        st.error(f"Erro ao ler o banco de dados: {e}")
        return pd.DataFrame()
    finally:
        if 'conn' in locals() and conn:
            conn.close()

def load_single_oc_details(oc_id: str) -> Dict[str, Any]:
    """Carrega todos os detalhes de uma única OC."""
    try:
        conn = _get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM ordens_compra WHERE OC_ID = ?", (oc_id,))
        row = cursor.fetchone()
        if row:
            # Converte a tupla em dict
            cols = [desc[0] for desc in cursor.description]
            return dict(zip(cols, row))
        return {}
    except Exception as e:
        st.error(f"Erro ao carregar detalhes da OC {oc_id}: {e}")
        return {}
    finally:
        if 'conn' in locals() and conn:
            conn.close()

def update_oc_status(oc_id: str, novo_status: str, df_itens_editado: pd.DataFrame):
    """Atualiza o status e os itens de uma OC no DB."""
    try:
        conn = _get_db_connection()
        
        # Prepara a atualização dos itens (se necessário)
        # (Lógica futura: salvar 'itens_recebidos_json')
        
        # Atualiza o status principal
        conn.execute("UPDATE ordens_compra SET STATUS = ? WHERE OC_ID = ?", (novo_status, oc_id))
        conn.commit()
        
        # Limpa o cache para forçar o recarregamento
        load_ocs_from_db.clear()
        
    except Exception as e:
        conn.rollback()
        st.error(f"Erro ao atualizar status da OC {oc_id}: {e}")
    finally:
        if 'conn' in locals() and conn:
            conn.close()

# --- FUNÇÃO PRINCIPAL DA ABA GERENCIADOR (FIX V10.15) ---
def display_gerenciador_interface(state):
    """Renderiza a Tab 5 (Gerenciador de OCs)"""
    
    st.subheader("✨ Gerenciador de OCs - Controle de Recebimento")
    
    if not os.path.exists(DB_FILE):
        st.info("Nenhuma Ordem de Compra foi salva ainda. Use a aba 'Ordem de Compra (OC)' para salvar sua primeira OC.")
        return

    # --- FILTROS ---
    col1, col2 = st.columns(2)
    filtro_empresa = col1.multiselect(
        "Filtrar Empresa",
        options=["ALIVVIA", "JCA"],
        default=["ALIVVIA", "JCA"]
    )
    filtro_status = col2.multiselect(
        "Filtrar Status",
        options=[STATUS_PENDENTE, STATUS_BAIXADA, STATUS_CANCELADA],
        default=[STATUS_PENDENTE]
    )

    if st.button("🔄 Recarregar OCs do Banco de Dados"):
        load_ocs_from_db.clear()
        st.success("Cache do Gerenciador de OCs limpo.")

    # --- CARREGA DADOS ---
    df_ocs = load_ocs_from_db(filtro_empresa, filtro_status)

    if df_ocs.empty:
        st.info("Nenhuma OC encontrada com os filtros selecionados.")
        return

    st.markdown(f"**{len(df_ocs)} OCs exibidas:**")

    # --- EDITOR PRINCIPAL (VISÃO GERAL) ---
    # Renomeia colunas para o display
    df_display = df_ocs.rename(columns={
        "OC_ID": "OC Nº",
        "DATA_OC": "Data Emissão",
        "DATA_PREVISTA": "Data Prevista",
        "VALOR_TOTAL_R": "Valor Total (R$)",
        "ITENS_COUNT": "Qtd. Itens"
    })

    edited_df = st.data_editor(
        df_display,
        key="editor_gerenciador_ocs",
        use_container_width=True,
        hide_index=True,
        disabled=df_display.columns # Desabilita edição direta aqui
    )
    
    st.markdown("---")
    
    # --- PAINEL DE AÇÕES (SELECIONAR OC) ---
    st.subheader("Ações e Detalhes da OC")
    
    oc_id_selecionada = st.selectbox(
        "Selecione uma OC para ver detalhes ou dar baixa:",
        options=df_ocs["OC_ID"].unique().tolist()
    )

    if not oc_id_selecionada:
        return

    # Carrega os detalhes completos da OC selecionada
    oc_details = load_single_oc_details(oc_id_selecionada)
    if not oc_details:
        return

    st.markdown(f"#### Detalhes da OC: **{oc_id_selecionada}**")
    
    # Mostra o HTML da OC
    html_oc = gerar_html_oc(oc_details)
    st.html(html_oc)
    
    st.download_button(
        label=f"💾 Baixar HTML da OC ({oc_id_selecionada})",
        data=html_oc,
        file_name=f"OC_{oc_id_selecionada}.html",
        mime="text/html",
    )
    
    st.markdown("---")
    st.markdown(f"#### Dar Baixa / Alterar Status (OC: {oc_id_selecionada})")
    
    # Carrega os itens da OC para um editor de baixa
    try:
        itens_oc = json.loads(oc_details.get("ITENS_JSON", "[]"))
        df_itens = pd.DataFrame(itens_oc)
        if "Qtd_Recebida" not in df_itens.columns:
            df_itens["Qtd_Recebida"] = 0
        if "NF_OK" not in df_itens.columns:
            df_itens["NF_OK"] = False
    except Exception as e:
        st.error(f"Erro ao ler itens JSON da OC: {e}")
        df_itens = pd.DataFrame()

    # =================================================================
    # >> INÍCIO DA CORREÇÃO (V10.15)
    # =================================================================
    if state.catalogo_df is not None:
        df_cat = state.catalogo_df[["sku", "fornecedor"]].rename(columns={"sku": "SKU"})
        df_itens = df_itens.merge(df_cat, on="SKU", how="left", suffixes=("_oc", "_cat"))
        df_itens["fornecedor"] = np.where(
            pd.isna(df_itens["fornecedor_oc"]) | (df_itens["fornecedor_oc"] == "N/A"),
            df_itens["fornecedor_cat"],
            df_itens["fornecedor_oc"]
        )
    
    cols_editor_baixa = [
        "fornecedor", "SKU", "Compra_Sugerida", "Preco", "Valor_Compra_R$",
        "Qtd_Recebida", "NF_OK"
    ]
    df_itens_display = df_itens[[col for col in cols_editor_baixa if col in df_itens.columns]].copy()
    # =================================================================
    # >> FIM DA CORREÇÃO (V10.15) <<
    # =================================================================

    st.markdown("##### Itens da OC:")
    df_itens_editado = st.data_editor(
        df_itens_display,
        key=f"editor_baixa_{oc_id_selecionada}",
        use_container_width=True,
        hide_index=True,
        column_config={
            "fornecedor": st.column_config.TextColumn(disabled=True),
            "SKU": st.column_config.TextColumn(disabled=True),
            "Compra_Sugerida": st.column_config.NumberColumn("Qtd. Pedida", format="%d", disabled=True),
            "Preco": st.column_config.NumberColumn("Preço R$", format="R$ %.2f", disabled=True),
            "Valor_Compra_R$": st.column_config.NumberColumn("Valor Total R$", format="R$ %.2f", disabled=True),
            "Qtd_Recebida": st.column_config.NumberColumn("Qtd. Recebida", min_value=0, format="%d"),
            "NF_OK": st.column_config.CheckboxColumn("NF OK?"),
        }
    )

    # Ações de Status
    c_b1, c_b2, c_b3 = st.columns(3)
    
    if c_b1.button("✅ Dar Baixa (Recebido)", key=f"baixa_{oc_id_selecionada}", type="primary"):
        update_oc_status(oc_id_selecionada, STATUS_BAIXADA, df_itens_editado)
        st.success(f"OC {oc_id_selecionada} marcada como '{STATUS_BAIXADA}'")
        st.rerun()

    if c_b2.button("⚠️ Voltar para Pendente", key=f"pendente_{oc_id_selecionada}"):
        update_oc_status(oc_id_selecionada, STATUS_PENDENTE, df_itens_editado)
        st.warning(f"OC {oc_id_selecionada} marcada como '{STATUS_PENDENTE}'")
        st.rerun()

    if c_b3.button("❌ Cancelar OC", key=f"cancelar_{oc_id_selecionada}"):
        update_oc_status(oc_id_selecionada, STATUS_CANCELADA, df_itens_editado)
        st.error(f"OC {oc_id_selecionada} marcada como '{STATUS_CANCELADA}'")
        st.rerun()