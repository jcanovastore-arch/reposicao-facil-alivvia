# mod_alocacao.py - TAB 3 - V10.5 (Compatível com V10.3)
# - Baseado na lógica interativa do V_old (reposicao_facil.py)
# - ADICIONA O BOTÃO "Enviar para Cesta"

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
)
from ordem_compra import adicionar_itens_cesta

def norm_sku(x: str) -> str:
    if pd.isna(x): return ""
    return unidecode(str(x)).strip().upper()

def _get_bytes_from_state(state, emp, k):
    """Função helper para buscar bytes da sessão (V10.3)"""
    slot_data = state.get(emp, {}).get(k, {})
    if slot_data.get("bytes"):
        return slot_data["name"], slot_data["bytes"]
    return None, None

def render_tab3(state):
    """Renderiza a Tab 3 (Alocação de Compra)"""
    st.subheader("Distribuir quantidade entre empresas — proporcional às vendas (FULL + Shopee)")

    if state.catalogo_df is None or state.kits_df is None:
        st.info("Carregue o **Padrão (KITS/CAT)** no sidebar.")
        return

    CATALOGO = state.catalogo_df
    sku_opcoes = CATALOGO["sku"].dropna().astype(str).sort_values().unique().tolist()
    sku_escolhido = st.selectbox("SKU do componente para alocar", sku_opcoes, key="alloc_sku")
    qtd_lote = st.number_input("Quantidade total do lote (ex.: 400)", min_value=1, value=1000, step=50)

    if st.button("Calcular alocação proporcional"):
        try:
            # precisa de FULL e VENDAS salvos para AMBAS as empresas
            missing = []
            files_map = {}
            for emp in ["ALIVVIA","JCA"]:
                for k in ["FULL", "VENDAS"]:
                    name, bytes_data = _get_bytes_from_state(state, emp, k)
                    if not (name and bytes_data):
                        missing.append(f"{emp} {k}")
                    else:
                        files_map[f"{emp}_{k}"] = (name, bytes_data)
                        
            if missing:
                raise RuntimeError("Faltam arquivos salvos: " + ", ".join(missing) + ". Use a aba **Dados das Empresas**.")

            # leitura BYTES
            def read_pair(emp: str) -> Tuple[pd.DataFrame,pd.DataFrame]:
                fa_name, fa_bytes = files_map[f"{emp}_FULL"]
                sa_name, sa_bytes = files_map[f"{emp}_VENDAS"]
                
                fa = load_any_table_from_bytes(fa_name, fa_bytes)
                sa = load_any_table_from_bytes(sa_name, sa_bytes)
                
                tfa = mapear_tipo(fa); tsa = mapear_tipo(sa)
                if tfa != "FULL":   raise RuntimeError(f"FULL inválido ({emp}): precisa de SKU e Vendas_60d/Estoque_full.")
                if tsa != "VENDAS": raise RuntimeError(f"Vendas inválido ({emp}): não achei coluna de quantidade.")
                return mapear_colunas(fa, tfa), mapear_colunas(sa, tsa)

            full_A, shp_A = read_pair("ALIVVIA")
            full_J, shp_J = read_pair("JCA")

            # explode por kits --> demanda 60d por componente
            cat = Catalogo(
                catalogo_simples=CATALOGO.rename(columns={"sku":"component_sku"}),
                kits_reais=state.kits_df
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

            demA = vendas_componente(full_A, shp_A)
            demJ = vendas_componente(full_J, shp_J)

            sku_norm = norm_sku(sku_escolhido)
            dA = int(demA.loc[demA["SKU"]==sku_norm, "Demanda_60d"].sum())
            dJ = int(demJ.loc[demJ["SKU"]==sku_norm, "Demanda_60d"].sum())

            total = dA + dJ
            if total == 0:
                st.warning("Sem vendas detectadas; alocação 50/50 por falta de base.")
                propA = propJ = 0.5
            else:
                propA = dA / total
                propJ = dJ / total

            alocA = int(round(qtd_lote * propA))
            alocJ = int(qtd_lote - alocA)

            res = pd.DataFrame([
                {"Empresa":"ALIVVIA", "SKU":sku_norm, "Demanda_60d":dA, "Proporção":round(propA,4), "Alocação_Sugerida":alocA},
                {"Empresa":"JCA",     "SKU":sku_norm, "Demanda_60d":dJ, "Proporção":round(propJ,4), "Alocação_Sugerida":alocJ},
            ])
            
            # Salva o resultado no estado para o botão de envio usar
            state.alocacao_calculada = res.to_dict("records")
            
            st.dataframe(res, use_container_width=True)
            st.success(f"Total alocado: {qtd_lote} un (ALIVVIA {alocA} | JCA {alocJ})")
            
            st.download_button("Baixar alocação (.csv)", data=res.to_csv(index=False).encode("utf-8"),
                               file_name=f"Alocacao_{sku_escolhido}_{qtd_lote}.csv", mime="text/csv")
        except Exception as e:
            st.error(str(e))

    # =================================================================
    # >> INÍCIO DA CORREÇÃO (V10.5) - BOTÃO DE ENVIO <<
    # =================================================================
    
    if "alocacao_calculada" in state and state.alocacao_calculada:
        st.markdown("---")
        if st.button("➡️ Enviar Alocação para Cesta de OC (Tab 4)", type="primary"):
            
            try:
                # Busca os dados de Preço e Fornecedor do Catálogo
                df_catalogo = state.catalogo_df
                if df_catalogo is None or df_catalogo.empty:
                    raise RuntimeError("Catálogo não está carregado.")
                
                # Pega o resultado salvo
                itens_alocados = state.alocacao_calculada
                
                # Transforma o resultado (2 linhas) em DataFrames para a cesta
                df_para_cesta = pd.DataFrame(itens_alocados)
                
                # Adiciona Preco e Fornecedor
                df_para_cesta = df_para_cesta.merge(
                    df_catalogo[['sku', 'fornecedor', 'Preco']].rename(columns={'sku':'SKU'}), # Assumindo que Preco está no catalogo
                    on="SKU",
                    how="left"
                )
                
                df_para_cesta = df_para_cesta.rename(columns={"Alocação_Sugerida": "Compra_Sugerida"})
                df_para_cesta["Preco"] = pd.to_numeric(df_para_cesta["Preco"], errors='coerce').fillna(0.0)

                # Separa por empresa
                df_alivvia = df_para_cesta[df_para_cesta["Empresa"] == "ALIVVIA"]
                df_jca = df_para_cesta[df_para_cesta["Empresa"] == "JCA"]

                # Envia para a cesta
                if not df_alivvia.empty:
                    adicionar_itens_cesta("ALIVVIA", df_alivvia)
                if not df_jca.empty:
                    adicionar_itens_cesta("JCA", df_jca)

                st.success("Alocação enviada para a Cesta de OC (Tab 4)!")
                del state.alocacao_calculada # Limpa o estado
                
            except Exception as e:
                st.error(f"Erro ao enviar alocação para cesta: {e}")
                st.error("Verifique se a coluna 'Preco' existe no seu 'CATALOGO_SIMPLES'.")