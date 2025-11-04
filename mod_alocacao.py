# mod_alocacao.py - TAB 3 - V10.12 (Reemissão V10.15)
# - FIX: Corrige o KeyError: "'Preco' not in index" (V10.10)
# - FIX: A função 'calcular_proporcoes_venda' agora lê os arquivos de ESTOQUE
#   para obter o "Preco", em vez de procurar no Catálogo.
# - Mantém o Novo Fluxo Manual (V10.10)
# - Mantém o Fix do @st.cache_data (V10.9)

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

@st.cache_data(show_spinner="Calculando proporções de venda...")
def calcular_proporcoes_venda(_state): # FIX V10.9: _state
    # (Função idêntica à V10.14 - omitida para brevidade)
    missing = []
    files_map = {}
    
    # AGORA LÊ O ESTOQUE TAMBÉM
    for emp in ["ALIVVIA","JCA"]:
        for k in ["FULL", "VENDAS", "ESTOQUE"]:
            name, bytes_data = _get_bytes_from_state(_state, emp, k)
            if not (name and bytes_data):
                missing.append(f"{emp} {k}")
            else:
                files_map[f"{emp}_{k}"] = (name, bytes_data)
                
    if missing:
        raise RuntimeError("Faltam arquivos: " + ", ".join(missing) + ". Use a aba **Dados das Empresas**.")

    # --- Leitura Vendas (FULL/Shopee) ---
    def read_vendas_pair(emp: str) -> tuple[pd.DataFrame, pd.DataFrame]:
        fa_name, fa_bytes = files_map[f"{emp}_FULL"]
        sa_name, sa_bytes = files_map[f"{emp}_VENDAS"]
        
        fa = load_any_table_from_bytes(fa_name, fa_bytes)
        sa = load_any_table_from_bytes(sa_name, sa_bytes)
        
        tfa = mapear_tipo(fa); tsa = mapear_tipo(sa)
        if tfa != "FULL":   raise RuntimeError(f"FULL inválido ({emp}): precisa de SKU e Vendas_60d.")
        if tsa != "VENDAS": raise RuntimeError(f"Vendas inválido ({emp}): não achei coluna de quantidade.")
        return mapear_colunas(fa, tfa), mapear_colunas(sa, tsa)

    full_A, shp_A = read_vendas_pair("ALIVVIA")
    full_J, shp_J = read_vendas_pair("JCA")

    # --- Leitura Estoque (para Preço) ---
    def read_estoque(emp: str) -> pd.DataFrame:
        e_name, e_bytes = files_map[f"{emp}_ESTOQUE"]
        e = load_any_table_from_bytes(e_name, e_bytes)
        te = mapear_tipo(e)
        if te != "FISICO": raise RuntimeError(f"Estoque inválido ({emp}): precisa de SKU, Estoque e Preço.")
        return mapear_colunas(e, te)

    fis_A = read_estoque("ALIVVIA")
    fis_J = read_estoque("JCA")
    
    # Combina os dois estoques para ter a melhor fonte de preços
    df_precos = pd.concat([fis_A, fis_J]).drop_duplicates(subset=["SKU"], keep="last")[["SKU", "Preco"]]

    # --- Cálculo de Proporção de Vendas ---
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

    demandas_finais = pd.merge(demA, demJ, on="SKU", how="outer").fillna(0)
    demandas_finais["Demanda_Total"] = demandas_finais["Demanda_A"] + demandas_finais["Demanda_J"]
    
    demandas_finais["Prop_A"] = demandas_finais.apply(
        lambda row: 0.5 if row["Demanda_Total"] == 0 else row["Demanda_A"] / row["Demanda_Total"],
        axis=1
    )
    demandas_finais["Prop_J"] = 1.0 - demandas_finais["Prop_A"]
    
    # --- Merge Final (com Catálogo para 'fornecedor' e df_precos para 'Preco') ---
    df_catalogo = _state.catalogo_df
    # 1. Adiciona 'fornecedor' do catálogo
    demandas_finais = demandas_finais.merge(
        df_catalogo[['sku', 'fornecedor']].rename(columns={'sku':'SKU'}),
        on="SKU",
        how="left"
    )
    # 2. Adiciona 'Preco' dos arquivos de estoque
    demandas_finais = demandas_finais.merge(
        df_precos,
        on="SKU",
        how="left"
    )
    
    demandas_finais["fornecedor"] = demandas_finais["fornecedor"].fillna("N/A")
    demandas_finais["Preco"] = pd.to_numeric(demandas_finais["Preco"], errors='coerce').fillna(0.0)
    
    return demandas_finais

def render_tab3(state):
    # (Função idêntica à V10.14 - omitida para brevidade)
    st.subheader("Alocação Manual de Compra")
    st.caption("Ferramenta independente para dividir um lote comprado (ex: 1000 blocos) proporcionalmente às vendas.")

    if state.catalogo_df is None or state.kits_df is None:
        st.info("Carregue o **Padrão (KITS/CAT)** no sidebar para usar esta ferramenta.")
        return

    try:
        # 1. Pega a lista de SKUs do catálogo
        sku_opcoes = state.catalogo_df["sku"].dropna().astype(str).sort_values().unique().tolist()
        
        # 2. UI: Seleção de SKU e Quantidade
        sku_escolhido_raw = st.selectbox("Selecione o SKU do componente para alocar", sku_opcoes, key="alloc_sku")
        qtd_lote = st.number_input("Quantidade total do lote comprado", min_value=1, value=1000, step=50)

        sku_escolhido = norm_sku(sku_escolhido_raw)

        if st.button("Calcular Alocação Proporcional", type="primary"):
            # 3. Calcula proporções para TODOS os SKUs (usa o cache)
            df_proporcoes_full = calcular_proporcoes_venda(state)
            
            # 4. Filtra o SKU escolhido
            df_sku = df_proporcoes_full[df_proporcoes_full["SKU"] == sku_escolhido].copy()
            
            if df_sku.empty:
                st.error(f"SKU '{sku_escolhido}' não encontrado nos dados de vendas. Verifique o Catálogo.")
                return

            item = df_sku.iloc[0]
            
            # 5. Calcula a alocação
            propA = item["Prop_A"]
            propJ = item["Prop_J"]
            
            alocA = int(round(qtd_lote * propA))
            alocJ = int(qtd_lote - alocA) # JCA fica com a diferença
            
            # 6. Salva no estado para o botão "Enviar"
            state.alocacao_manual_calculada = [
                {
                    "SKU": sku_escolhido,
                    "fornecedor": item["fornecedor"],
                    "Preco": item["Preco"],
                    "Compra_Sugerida": alocA,
                    "Empresa": "ALIVVIA"
                },
                {
                    "SKU": sku_escolhido,
                    "fornecedor": item["fornecedor"],
                    "Preco": item["Preco"],
                    "Compra_Sugerida": alocJ,
                    "Empresa": "JCA"
                }
            ]

            # 7. Exibe o resultado
            st.success(f"Alocação para {qtd_lote} unidades de **{sku_escolhido}**:")
            res = pd.DataFrame([
                {"Empresa": "ALIVVIA", "Demanda_60d": item["Demanda_A"], "Proporção": propA, "Alocação_Sugerida": alocA, "Preço": item["Preco"]},
                {"Empresa": "JCA", "Demanda_60d": item["Demanda_J"], "Proporção": propJ, "Alocação_Sugerida": alocJ, "Preço": item["Preco"]},
            ])
            st.dataframe(res, use_container_width=True, column_config={
                "Demanda_60d": st.column_config.NumberColumn(format="%d"),
                "Proporção": st.column_config.ProgressColumn(format="%.1f%%", min_value=0, max_value=1),
                "Alocação_Sugerida": st.column_config.NumberColumn(format="%d"),
                "Preço": st.column_config.NumberColumn(format="R$ %.2f"),
            })

        # 8. Botão de Envio (se o cálculo foi feito)
        if "alocacao_manual_calculada" in state:
            st.markdown("---")
            if st.button("➡️ Enviar esta Alocação para Cesta de OC (Tab 4)", type="secondary"):
                try:
                    itens = state.alocacao_manual_calculada
                    
                    df_alivvia = pd.DataFrame([i for i in itens if i["Empresa"] == "ALIVVIA"])
                    df_jca = pd.DataFrame([i for i in itens if i["Empresa"] == "JCA"])

                    if not df_alivvia.empty:
                        adicionar_itens_cesta("ALIVVIA", df_alivvia)
                    if not df_jca.empty:
                        adicionar_itens_cesta("JCA", df_jca)
                    
                    st.success("Alocação manual enviada para a Cesta de OC (Tab 4)!")
                    del state.alocacao_manual_calculada # Limpa
                    
                except Exception as e:
                    st.error(f"Erro ao enviar alocação para cesta: {e}")

    except Exception as e:
        st.error(f"Erro ao carregar Alocação: {e}")