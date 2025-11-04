# mod_compra_autom.py - TAB 2 - V10.6 (Compatível com V10.3)
# - CORREÇÃO CRÍTICA: Remove o st.rerun() que impedia o salvamento da cesta.

import pandas as pd
import streamlit as st
import numpy as np

import logica_compra
from logica_compra import (
    Catalogo,
    aggregate_data_for_conjunta_clean,
    load_any_table_from_bytes,
    mapear_colunas,
    mapear_tipo,
    exportar_xlsx,
    calcular as calcular_compra,
)

# IMPORTAÇÃO CRÍTICA
try:
    from ordem_compra import adicionar_itens_cesta
except ImportError:
    # Fallback se a função não for encontrada
    def adicionar_itens_cesta(empresa: str, df: pd.DataFrame):
        st.error("Falha crítica: Função 'adicionar_itens_cesta' não encontrada em ordem_compra.py")

# Funções de verificação (do V9.1)
def _require_cols(df: pd.DataFrame, cols: list[str], ctx: str):
    faltando = [c for c in cols if c not in df.columns]
    if faltando:
        raise RuntimeError(f"[{ctx}] Faltam colunas: {', '.join(faltando)}")

def _safe_contains_series(series: pd.Series, text: str) -> pd.Series:
    try:
        return series.fillna("").astype(str).str.contains(text, case=False, na=False)
    except Exception:
        return pd.Series([False]*len(series), index=series.index)

# Função principal (Render)
def render_tab2(state, h, g, LT):
    st.subheader("Gerar Compra (por empresa ou conjunta) — lógica original")

    if state.catalogo_df is None or state.kits_df is None:
        st.info("Carregue o **Padrão (KITS/CAT)** no sidebar antes de usar as abas.")
        return

    empresa_selecionada = st.radio("Empresa ativa", ["ALIVVIA", "JCA", "CONJUNTA"], horizontal=True, key="empresa_ca")
    nome_estado = empresa_selecionada

    # (Lógica de display visual - inalterada)
    if nome_estado == "CONJUNTA":
        st.info("Arquivos agregados prontos para o cálculo Conjunto.")
    else:
        dados_display = state.get(nome_estado, {})
        col = st.columns(3)
        col[0].info(f"FULL: {dados_display.get('FULL', {}).get('name') or '—'}")
        col[1].info(f"Shopee/MT: {dados_display.get('VENDAS', {}).get('name') or '—'}")
        col[2].info(f"Estoque: {dados_display.get('ESTOQUE', {}).get('name') or '—'}")

    if st.button(f"Gerar Compra — {nome_estado}", type="primary"):
        state.compra_autom_data["force_recalc"] = True

    # (Lógica de Cálculo - V9.1 - Inalterada)
    if nome_estado not in state.compra_autom_data or state.compra_autom_data.get("force_recalc", False):
        state.compra_autom_data["force_recalc"] = False
        try:
            cat = Catalogo(
                catalogo_simples=state.catalogo_df.rename(columns={"sku": "component_sku"}),
                kits_reais=state.kits_df
            )

            if nome_estado == "CONJUNTA":
                dfs = {}
                missing = []
                for emp in ("ALIVVIA", "JCA"):
                    dados = state.get(emp, {})
                    for k, rot in (("FULL","FULL"), ("VENDAS","Shopee/MT"), ("ESTOQUE","Estoque")):
                        slot_data = dados.get(k, {})
                        if not (slot_data.get("name") and slot_data.get("bytes")):
                            missing.append(f"{emp} {rot}")
                            continue
                        
                        # O V10.3 (reposicao_facil) garante que os bytes estão na sessão
                        raw_bytes = slot_data["bytes"]
                        if raw_bytes is None:
                            # Tenta recarregar do disco (se o V10.3 falhou na pré-carga)
                            try:
                                disk_item = logica_compra.load_from_disk_if_any(emp, k) 
                                if disk_item:
                                    raw_bytes = disk_item["bytes"]
                                    state[emp][k]["bytes"] = raw_bytes # Salva na RAM
                            except Exception:
                                pass # Ignora se a função load_from_disk_if_any não estiver em logica_compra
                        
                        if raw_bytes is None:
                            missing.append(f"{emp} {rot} (bytes não carregados)")
                            continue
                            
                        raw = load_any_table_from_bytes(slot_data["name"], raw_bytes)
                        tipo = mapear_tipo(raw)
                        
                        if tipo == "FULL": dfs[f"full_{emp[0]}"] = mapear_colunas(raw, tipo)
                        elif tipo == "VENDAS": dfs[f"vend_{emp[0]}"] = mapear_colunas(raw, tipo)
                        elif tipo == "FISICO": dfs[f"fisi_{emp[0]}"] = mapear_colunas(raw, tipo)
                        else: raise RuntimeError(f"Arquivo {rot} de {emp} com formato incorreto: {tipo}.")

                if missing:
                    raise RuntimeError(
                        "Arquivos necessários para Compra Conjunta ausentes: "
                        + ", ".join(missing)
                        + ". Recarregue todos na aba 'Dados das Empresas'."
                    )

                full_df, fisico_df, vendas_df = aggregate_data_for_conjunta_clean(
                    dfs["full_A"], dfs["vend_A"], dfs["fisi_A"],
                    dfs["full_J"], dfs["vend_J"], dfs["fisi_J"]
                )
                nome_empresa_calc = "CONJUNTA"

            else: # ALIVVIA ou JCA
                dados = state.get(nome_estado, {})
                missing = []
                for k, rot in (("FULL","FULL"),("VENDAS","Shopee/MT"),("ESTOQUE","Estoque")):
                    slot_data = dados.get(k, {})
                    if not (slot_data.get("name") and slot_data.get("bytes")):
                        missing.append(f"{nome_estado} {rot}")
                if missing:
                    raise RuntimeError(
                        f"Arquivos necessários ausentes: {', '.join(missing)}. "
                        f"Vá em