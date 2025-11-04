# mod_alocacao.py - TAB 3 - V10.9
# - REATIVA a Tab 3 (remove o "Desativado" do V10.7)
# - FIX: Corrige o crash "@st.cache_data" (Cannot hash argument 'state')
# - Mantém o fluxo V10.6 (Lê da Tab 2, divide, envia para OC)

import streamlit as st
import pandas as pd
import numpy as np
from unidecode import unidecode

# Importa as funções necessárias
from logica_compra import (
    Catalogo,
    load_any_table_from_bytes,
    mapear_tipo,
    mapear_colunas,
    explodir_por_kits,
    construir_kits_efetivo,
    norm_sku
)
try:
    from ordem_compra import adicionar_itens_cesta
except ImportError:
    def adicionar_itens_cesta(empresa: str, df: pd.DataFrame):
        st.error("Falha crítica: Função 'adicionar_itens_cesta' não encontrada em ordem_compra.py")

def _get_bytes_from_state(state, emp, k):
    """Função helper para buscar bytes da sessão (V10.3)"""
    slot_data = state.get(emp, {}).get(k, {})
    if slot_data.get("bytes"):
        return slot_data["name"], slot_data["bytes"]
    return None, None

# =================================================================
# >> INÍCIO DA CORREÇÃO (V10.9) - Crash do @st.cache_data <<
# =================================================================
@st.cache_data(show_spinner="Calculando proporções de venda...")
def calcular_proporcoes_venda(_state): # <- 'state' mudou para '_state'
    """
    Calcula a demanda de 60 dias por componente para ALIVVIA e JCA.
    Retorna um DataFrame mesclado com a proporção.
    """
    missing = []
    files_map = {}
    for emp in ["ALIVVIA","JCA"]:
        for k in ["FULL", "VENDAS"]:
            # Lê do _state (que é o st.session_state)
            name, bytes_data = _get_bytes_from_state(_state, emp, k)
            if not (name and bytes_data):
                missing.append(f"{emp} {k}")
            else:
                files_map[f"{emp}_{k}"] = (name, bytes_data)
                
    if missing:
        raise RuntimeError("Faltam arquivos de vendas: " + ", ".join(missing) + ". Use a aba **Dados das Empresas**.")

    # Leitura BYTES
    def read_pair(emp: str) -> tuple[pd.DataFrame, pd.DataFrame]:
        fa_name, fa_bytes = files_map[f"{emp}_FULL"]
        sa_name, sa_bytes = files_map[f"{emp}_VENDAS"]
        
        fa = load_any_table_from_bytes(fa_name, fa_bytes)
        sa = load_any_table_from_bytes(sa_name, sa_bytes)
        
        tfa = mapear_tipo(fa); tsa = mapear_tipo(sa)
        if tfa != "FULL":   raise RuntimeError(f"FULL inválido ({emp}): precisa de SKU e Vendas_60d.")
        if tsa != "VENDAS": raise RuntimeError(f"Vendas inválido ({emp}): não achei coluna de quantidade.")
        return mapear_colunas(fa, tfa), mapear_colunas(sa, tsa)

    full_A, shp_A = read_pair("ALIVVIA")
    full_J, shp_J = read_pair("JCA")

    # Explode por kits
    cat = Catalogo(
        catalogo_simples=_state.catalogo_df.rename(columns={"sku":"component_sku"}),
        kits_reais=_state.kits_df
    )
    kits = construir_kits_efetivo(cat)

    def vendas_componente(full_df, shp_df) -> pd.DataFrame:
        a = explodir_por_kits(full_df[["SKU","Vendas_Qtd_60d"]].rename(columns={"SKU":"kit_sku","Vendas_Qtd_60d":"Qtd"}), kits,"kit_sku","Qtd")
        a = a.rename(columns={"Quantidade":"ML_60d"})
        b = explodir_por_kits(shp_df[["SKU","Quantidade"]].rename(columns={"SKU":"kit_sku","Quantidade":"Qtd"}), kits,"kit_sku","Qtd")
        b = b.rename(columns={"Quantidade":"Shopee_60d"})
        out = pd.merge(a, b, on="SKU", how="outer").fillna(0)
        out["Demanda_60d"] = out["ML_60d"].astype(int) + out["Shopee_60d"].astype(int)
        return out[["SKU","Demanda_60d"]]

    demA = vendas_componente(full_A, shp_A).rename(columns={"Demanda_60d": "Demanda_A"})
    demJ = vendas_componente(full_J, shp_J).rename(columns={"Demanda_60d": "Demanda_J"})

    # Junta as demandas
    demandas_finais = pd.merge(demA, demJ, on="SKU", how="outer").fillna(0)
    demandas_finais["Demanda_Total"] = demandas_finais["Demanda_A"] + demandas_finais["Demanda_J"]
    
    # Calcula proporções (evita divisão por zero)
    demandas_finais["Prop_A"] = demandas_finais.apply(
        lambda row: 0.5 if row["Demanda_Total"] == 0 else row["Demanda_A"] / row["Demanda_Total"],
        axis=1
    )
    demandas_finais["Prop_J"] = 1.0 - demandas_finais["Prop_A"]
    
    return demandas_finais[["SKU", "Demanda_A", "Demanda_J", "Demanda_Total", "Prop_A", "Prop_J"]]
# =================================================================
# >> FIM DA CORREÇÃO (V10.9) <<
# =================================================================


def render_tab3(state):
    """Renderiza a Tab 3 (Alocação de Compra)"""
    st.subheader("Distribuir 'Compra Conjunta' proporcional às vendas")

    # 1. Verifica se a "Compra Conjunta" (Tab 2) foi calculada
    if "CONJUNTA" not in state.compra_autom_data or "df" not in state.compra_autom_data["CONJUNTA"]:
        st.info("Calcule a **'Compra CONJUNTA'** na **Tab 2 (Compra Automática)** primeiro para usar esta aba.")
        return

    df_conjunta_raw = state.compra_autom_data["CONJUNTA"]["df"].copy()
    df_conjunta = df_conjunta_raw[df_conjunta_raw["Compra_Sugerida"] > 0].copy()

    if df_conjunta.empty:
        st.success("Cálculo 'Compra Conjunta' (Tab 2) não sugeriu nenhuma compra.")
        return

    try:
        # 2. Calcula as proporções de venda (passando 'state' para a função cacheada)
        df_proporcoes = calcular_proporcoes_venda(state)

        # 3. Mescla os resultados
        df_alocacao = pd.merge(
            df_conjunta,
            df_proporcoes[["SKU", "Prop_A", "Prop_J"]],
            on="SKU",
            how="left"
        )
        
        # Preenche SKUs sem vendas com 50/50
        df_alocacao["Prop_A"] = df_alocacao["Prop_A"].fillna(0.5)
        df_alocacao["Prop_J"] = df_alocacao["Prop_J"].fillna(0.5)

        # 4. Calcula a alocação final
        df_alocacao["ALIVVIA (Qtd)"] = (df_alocacao["Compra_Sugerida"] * df_alocacao["Prop_A"]).round().astype(int)
        # JCA fica com a diferença para garantir que o total bate
        df_alocacao["JCA (Qtd)"] = df_alocacao["Compra_Sugerida"] - df_alocacao["ALIVVIA (Qtd)"]
        
        df_alocacao["ALIVVIA (R$)"] = (df_alocacao["ALIVVIA (Qtd)"] * df_alocacao["Preco"]).round(2)
        df_alocacao["JCA (R$)"] = (df_alocacao["JCA (Qtd)"] * df_alocacao["Preco"]).round(2)

        # 5. Salva no estado para o botão "Enviar"
        state.alocacao_calculada_final = df_alocacao.to_dict("records")
        
        # 6. Renderiza a tabela (com formatação)
        st.success("Compra Conjunta carregada. Abaixo a sugestão de alocação proporcional às vendas (60d).")
        
        cols_display = [
            "SKU", "fornecedor", 
            "Estoque_Fisico", "TOTAL_60d", 
            "Compra_Sugerida", "Valor_Compra_R$",
            "Prop_A", "ALIVVIA (Qtd)", "ALIVVIA (R$)",
            "Prop_J", "JCA (Qtd)", "JCA (R$)"
        ]
        
        df_display = df_alocacao[[col for col in cols_display if col in df_alocacao.columns]].copy()
        
        # FORMATAÇÃO (V10.7)
        st.dataframe(
            df_display,
            use_container_width=True,
            height=600,
            column_config={
                "Estoque_Fisico": st.column_config.NumberColumn("Estoque Físico", format="%d"),
                "TOTAL_60d": st.column_config.NumberColumn("Vendas 60d", format="%d"),
                "Compra_Sugerida": st.column_config.NumberColumn("Compra Sugerida (Total)", format="%d"),
                "Valor_Compra_R$": st.column_config.NumberColumn("Valor Total R$", format="R$ %.2f"),
                "Prop_A": st.column_config.ProgressColumn("Prop. ALIVVIA", format="%.1f%%", min_value=0, max_value=1),
                "ALIVVIA (Qtd)": st.column_config.NumberColumn("ALIVVIA (Qtd)", format="%d"),
                "ALIVVIA (R$)": st.column_config.NumberColumn("ALIVVIA (R$)", format="R$ %.2f"),
                "Prop_J": st.column_config.ProgressColumn("Prop. JCA", format="%.1f%%", min_value=0, max_value=1),
                "JCA (Qtd)": st.column_config.NumberColumn("JCA (Qtd)", format="%d"),
                "JCA (R$)": st.column_config.NumberColumn("JCA (R$)", format="R$ %.2f"),
            }
        )
        
        st.markdown("---")
        if st.button("➡️ Enviar Alocação DIVIDIDA para Cesta de OC (Tab 4)", type="primary"):
            
            try:
                itens_dict = state.get("alocacao_calculada_final", [])
                if not itens_dict:
                    st.error("Nenhum item de alocação encontrado no estado.")
                    return

                df_final_alocacao = pd.DataFrame(itens_dict)
                
                df_alivvia = df_final_alocacao.rename(columns={"ALIVVIA (Qtd)": "Compra_Sugerida"})
                df_alivvia = df_alivvia[df_alivvia["Compra_Sugerida"] > 0]
                
                df_jca = df_final_alocacao.rename(columns={"JCA (Qtd)": "Compra_Sugerida"})
                df_jca = df_jca[df_jca["Compra_Sugerida"] > 0]

                # Envia para a cesta
                if not df_alivvia.empty:
                    adicionar_itens_cesta("ALIVVIA", df_alivvia)
                if not df_jca.empty:
                    adicionar_itens_cesta("JCA", df_jca)

                st.success("Alocação enviada para a Cesta de OC (Tab 4)!")
                
                # Limpa o estado para evitar re-envio
                del state.alocacao_calculada_final
                del state.compra_autom_data["CONJUNTA"]
                
            except Exception as e:
                st.error(f"Erro ao enviar alocação para cesta: {e}")

    except Exception as e:
        st.error(f"Erro ao calcular alocação: {e}")