# gerenciador_oc.py - Gerenciador de OCs (AGORA COM SQLITE)
import streamlit as st
import pandas as pd
import json
import sqlite3
import datetime as dt

# Importar funções de persistência e impressão do módulo de OC
from ordem_compra import _get_db_connection, gerar_html_oc, STATUS_PENDENTE, STATUS_BAIXADA, STATUS_CANCELADA

# --- FUNÇÕES DE PERSISTÊNCIA ---

@st.cache_data(ttl=5) 
def listar_ocs_cached():
    """Carrega todas as OCs do banco de dados (com cache)."""
    conn = _get_db_connection()
    try:
        df = pd.read_sql_query("SELECT * FROM ordens_compra", conn)
        conn.close()
        
        if df.empty: return pd.DataFrame()
            
        # Conversões de tipo
        df['VALOR_TOTAL_R$'] = pd.to_numeric(df['VALOR_TOTAL_R'], errors='coerce').fillna(0.0).round(2)
        df['DATA_OC'] = pd.to_datetime(df['DATA_OC'], errors='coerce').dt.date
        df = df.rename(columns={'VALOR_TOTAL_R': 'VALOR_TOTAL_R$'})
        df = df.sort_values('OC_ID', ascending=False).reset_index(drop=True)
        
        return df
        
    except Exception as e:
        st.error(f"Erro ao listar OCs do Banco de Dados: {e}")
        return pd.DataFrame()

def update_oc_status_in_db(oc_id: str, novo_status: str):
    """Atualiza o STATUS de uma OC específica no banco de dados."""
    conn = _get_db_connection()
    try:
        conn.execute("UPDATE ordens_compra SET STATUS = ? WHERE OC_ID = ?", (novo_status.upper(), oc_id))
        conn.commit()
        
        st.success(f"✅ Status da OC **{oc_id}** atualizado para **{novo_status.upper()}** no Banco de Dados!")
        
        listar_ocs_cached.clear()
        
    except Exception as e:
        conn.rollback()
        st.error(f"Falha ao atualizar o status da OC {oc_id}: {e}")
    finally:
        conn.close()

# --- INTERFACE (Lógica inalterada) ---

def display_oc_manager():
    """Renderiza a interface do Gerenciador de Ordens de Compra (OCs)."""
    
    st.title("✨ Gerenciador de OCs - Controle de Recebimento")
    
    if st.button("🔄 Recarregar OCs do Banco de Dados", key="btn_reload_ocs", type="secondary"):
        listar_ocs_cached.clear()
        st.experimental_rerun()
        
    df_ocs = listar_ocs_cached()
    
    if df_ocs.empty:
        st.warning("Nenhuma Ordem de Compra encontrada. Salve a primeira OC na aba anterior.")
        return

    # Filtros
    colF1, colF2 = st.columns(2)
    empresas = sorted(df_ocs["EMPRESA"].unique().tolist())
    status_list = sorted(df_ocs["STATUS"].unique().tolist())
    
    default_status = [s for s in status_list if s in [STATUS_PENDENTE, STATUS_CANCELADA]]
    
    filtro_empresa = colF1.multiselect("Filtrar Empresa", empresas, default=empresas)
    filtro_status = colF2.multiselect("Filtrar Status", status_list, default=default_status)
    
    df_filtrado = df_ocs[df_ocs["EMPRESA"].isin(filtro_empresa)].copy()
    df_filtrado = df_filtrado[df_filtrado["STATUS"].isin(filtro_status)]

    st.caption(f"OCs exibidas: {len(df_filtrado)}")
    
    # Adiciona a coluna de Ação para renderizar o botão
    df_filtrado["Ações"] = df_filtrado.apply(lambda row: 
        "✅ Dar Baixa" if row['STATUS'] == STATUS_PENDENTE else row['STATUS'], axis=1
    )
    
    df_display = df_filtrado[[
        "OC_ID", "EMPRESA", "FORNECEDOR", "DATA_OC", "VALOR_TOTAL_R$", "STATUS", "Ações", "ITENS_JSON"
    ]].rename(columns={"DATA_OC": "EMISSÃO"})
    
    # O data_editor permite marcar e desmarcar ações
    df_acoes = st.data_editor(
        df_display.drop(columns=['ITENS_JSON']), # Remove coluna JSON para visualização
        use_container_width=True,
        hide_index=True,
        column_config={
            "Ações": st.column_config.SelectboxColumn(
                "Ação", 
                options=["✅ Dar Baixa", STATUS_PENDENTE, STATUS_BAIXADA, STATUS_CANCELADA], 
                default="PENDENTE",
                width="small",
                help="Selecione 'Dar Baixa' ou 'Cancelar'."
            ),
            "VALOR_TOTAL_R$": st.column_config.NumberColumn("Valor (R$)", format="R$ %.2f")
        },
        key="editor_ocs_acoes"
    )
    
    # --- PROCESSAR AÇÕES DE BAIXA/CANCELAMENTO ---
    
    ocs_para_baixar = df_acoes[df_acoes["Ações"] == "✅ Dar Baixa"].copy()
    ocs_para_cancelar = df_acoes[df_acoes["Ações"] == STATUS_CANCELADA].copy()

    if not ocs_para_baixar.empty:
        st.markdown("---")
        st.error(f"⚠️ **CONFIRMAR BAIXA:** Confirme que os {len(ocs_para_baixar)} itens abaixo chegaram para fechar a OC.")
        
        for oc_id in ocs_para_baixar["OC_ID"]:
            if st.button(f"CONFIRMAR BAIXA e FECHAR OC {oc_id}", key=f"confirm_baixa_{oc_id}", type="primary"):
                update_oc_status_in_db(oc_id, STATUS_BAIXADA)
                st.experimental_rerun()
                
    if not ocs_para_cancelar.empty:
        st.markdown("---")
        st.warning(f"⚠️ **CONFIRMAR CANCELAMENTO:** Confirme que deseja cancelar {len(ocs_para_cancelar)} OCs.")
        
        for oc_id in ocs_para_cancelar["OC_ID"]:
            if st.button(f"CONFIRMAR CANCELAMENTO OC {oc_id}", key=f"confirm_cancel_{oc_id}"):
                update_oc_status_in_db(oc_id, STATUS_CANCELADA)
                st.experimental_rerun()


    # --- DETALHES E IMPRESSÃO (ABAIXO DA TABELA) ---
    st.markdown("---")
    
    # Garante que os IDs para o selectbox vêm do DF original completo para evitar erro
    oc_ids_full = df_ocs["OC_ID"].tolist() 
    if oc_ids_full:
        oc_selecionada_id = st.selectbox("Selecione a OC para Visualizar / Imprimir:", options=oc_ids_full)
        
        full_oc_data = df_ocs[df_ocs["OC_ID"] == oc_selecionada_id].iloc[0].to_dict()
        
        # O campo DATA_OC pode ser um objeto date, precisa ser string para a função de impressão
        if isinstance(full_oc_data.get("DATA_OC"), dt.date):
             full_oc_data["DATA_OC"] = full_oc_data["DATA_OC"].strftime("%Y-%m-%d")
        if isinstance(full_oc_data.get("DATA_PREVISTA"), dt.date):
             full_oc_data["DATA_PREVISTA"] = full_oc_data["DATA_PREVISTA"].strftime("%Y-%m-%d")


        html_content = gerar_html_oc(full_oc_data)
        
        col_print, col_visual = st.columns([1, 2])
        
        with col_print:
            st.download_button(
                label=f"📄 Imprimir OC {oc_selecionada_id} (HTML A4)",
                data=html_content,
                file_name=f"OC_{oc_selecionada_id}.html",
                mime="text/html",
                key=f"download_{oc_selecionada_id}",
                use_container_width=True
            )
        
        with col_visual:
            st.info(f"Fornecedor: **{full_oc_data['FORNECEDOR']}** | Status: **{full_oc_data['STATUS']}**")
            st.markdown("Use o botão ao lado para baixar o arquivo pronto para impressão.")
    else:
        st.info("Nenhuma OC disponível para visualização.")