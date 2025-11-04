# mod_compra_autom.py - TAB 2 - V10.12 (Reemissão V10.15)
# - FIX: Corrige o KeyError: 'fornecedor - ALIVVIA' (V10.10) na lógica de merge.
# - Mantém o Novo Fluxo Conjunta (Mesclado + Botão Inteligente)
# - Mantém o Fix do @st.cache_data (V10.9)
# - Mantém o Fix do Stale Cache (V10.11)

import pandas as pd
import streamlit as st
import numpy as np
from unidecode import unidecode # Necessário para norm_sku

import logica_compra
from logica_compra import (
    Catalogo,
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
    def adicionar_itens_cesta(empresa: str, df: pd.DataFrame):
        st.error("Falha crítica: Função 'adicionar_itens_cesta' não encontrada em ordem_compra.py")

def norm_sku(x: str) -> str:
    if pd.isna(x): return ""
    return unidecode(str(x)).strip().upper()

def _require_cols(df: pd.DataFrame, cols: list[str], ctx: str):
    faltando = [c for c in cols if c not in df.columns]
    if faltando:
        raise RuntimeError(f"[{ctx}] Faltam colunas: {', '.join(faltando)}")

def _safe_contains_series(series: pd.Series, text: str) -> pd.Series:
    try:
        return series.fillna("").astype(str).str.contains(text, case=False, na=False)
    except Exception:
        return pd.Series([False]*len(series), index=series.index)


@st.cache_data(show_spinner="Calculando Compra para _empresa_...")
def calcular_compra_para_empresa(_empresa_, _state, h, g, LT): # FIX V10.9: _state
    """
    Função cacheada que executa a lógica de cálculo para UMA empresa.
    O argumento '_state' é ignorado pelo cache.
    """
    dados = _state.get(_empresa_, {})
    missing = []
    for k, rot in (("FULL","FULL"),("VENDAS","Shopee/MT"),("ESTOQUE","Estoque")):
        slot_data = dados.get(k, {})
        if not (slot_data.get("name") and slot_data.get("bytes")):
            missing.append(f"{_empresa_} {rot}")
    if missing:
        raise RuntimeError(
            f"Arquivos necessários ausentes: {', '.join(missing)}. "
            f"Vá em **Dados das Empresas** e confirme o upload."
        )

    full_raw   = load_any_table_from_bytes(dados["FULL"]["name"],    dados["FULL"]["bytes"])
    vendas_raw = load_any_table_from_bytes(dados["VENDAS"]["name"],  dados["VENDAS"]["bytes"])
    fisico_raw = load_any_table_from_bytes(dados["ESTOQUE"]["name"], dados["ESTOQUE"]["bytes"])

    t_full = mapear_tipo(full_raw)
    t_v    = mapear_tipo(vendas_raw)
    t_f    = mapear_tipo(fisico_raw)
    if t_full != "FULL" or t_v != "VENDAS" or t_f != "FISICO":
        raise RuntimeError(f"Um ou mais arquivos (FULL/VENDAS/FISICO) de {_empresa_} estão com formato incorreto.")

    full_df   = mapear_colunas(full_raw, t_full)
    vendas_df = mapear_colunas(vendas_raw, t_v)
    fisico_df = mapear_colunas(fisico_raw, t_f)
    
    cat = Catalogo(
        catalogo_simples=_state.catalogo_df.rename(columns={"sku": "component_sku"}),
        kits_reais=_state.kits_df
    )
    
    df_final, painel = calcular_compra(full_df, fisico_df, vendas_df, cat, h=h, g=g, LT=LT)
    
    for col_req in ("SKU", "fornecedor", "Compra_Sugerida", "Preco"):
        if col_req not in df_final.columns:
            raise RuntimeError(f"[Pós-cálculo] Coluna ausente: {col_req}")

    if "Selecionar" not in df_final.columns:
        df_final["Selecionar"] = False
    
    return df_final, painel


def renderizar_painel_individual(df_final, painel, nome_empresa_calc, state):
    """
    Renderiza a tabela (formatada) e o botão de envio para UMA empresa (Alivvia ou JCA).
    """
    cA, cB, cC, cD = st.columns(4)
    cA.metric("Full (un)",   f"{int(painel.get('full_unid', 0)):,}".replace(",", "."))
    cB.metric("Full (R$)",   f"R$ {float(painel.get('full_valor', 0.0)):,.2f}")
    cC.metric("Físico (un)", f"{int(painel.get('fisico_unid', 0)):,}".replace(",", "."))
    cD.metric("Físico (R$)", f"R$ {float(painel.get('fisico_valor', 0.0)):,.2f}")

    c_f1, c_f2 = st.columns(2)
    _require_cols(df_final, ["fornecedor", "SKU", "Compra_Sugerida"], "Filtros/Render")
    fornecedores = sorted(df_final["fornecedor"].fillna("").astype(str).unique().tolist())
    filtro_forn = c_f1.multiselect("Filtrar Fornecedor", fornecedores, key=f"filtro_forn_{nome_empresa_calc}")
    filtro_sku_text = c_f2.text_input("Buscar SKU/Parte do SKU", key=f"filtro_sku_{nome_empresa_calc}").strip()

    df_filtrado = df_final.copy()
    if filtro_forn:
        df_filtrado = df_filtrado[df_filtrado["fornecedor"].isin(filtro_forn)]
    if filtro_sku_text:
        df_filtrado = df_filtrado[_safe_contains_series(df_filtrado["SKU"], filtro_sku_text)]

    base_para_editor = df_filtrado[df_filtrado["Compra_Sugerida"] > 0].reset_index(drop=True).copy()
    if "Selecionar" not in base_para_editor.columns:
        base_para_editor["Selecionar"] = False

    editor_key = f"data_editor_{nome_empresa_calc}"
    
    cols_display = [
        "Selecionar", "SKU", "fornecedor",
        "Vendas_h_ML", "Vendas_h_Shopee", "TOTAL_60d",
        "Estoque_Fisico", "Preco", 
        "Compra_Sugerida", "Valor_Compra_R$"
    ]
    df_display = base_para_editor[[col for col in cols_display if col in base_para_editor.columns]].copy()

    edited_df = st.data_editor(
        df_display,
        key=editor_key,
        use_container_width=True,
        height=400,
        column_config={
            "Selecionar": st.column_config.CheckboxColumn("Selecionar", default=False),
            "Vendas_h_ML": st.column_config.NumberColumn("Vendas ML (h)", format="%d"),
            "Vendas_h_Shopee": st.column_config.NumberColumn("Vendas Shopee (h)", format="%d"),
            "TOTAL_60d": st.column_config.NumberColumn("Vendas 60d", format="%d"),
            "Estoque_Fisico": st.column_config.NumberColumn("Estoque Físico", format="%d"),
            "Preco": st.column_config.NumberColumn("Preço R$", format="R$ %.2f"),
            "Compra_Sugerida": st.column_config.NumberColumn("Compra Sugerida", format="%d"),
            "Valor_Compra_R$": st.column_config.NumberColumn("Valor Compra R$", format="R$ %.2f"),
        }
    )

    df_selecionados = pd.DataFrame()
    try:
        if isinstance(edited_df, pd.DataFrame) and "Selecionar" in edited_df.columns:
            df_selecionados = base_para_editor.merge(
                edited_df[edited_df["Selecionar"] == True][["SKU", "Selecionar"]],
                on="SKU",
                how="inner",
                suffixes=('', '_sel')
            ).drop(columns=['Selecionar_sel'])
    except Exception:
        df_selecionados = pd.DataFrame()

    qtd_sel = 0 if df_selecionados is None or df_selecionados.empty else len(df_selecionados)

    if qtd_sel == 0:
        st.button("Enviar 0 itens selecionados para a Cesta de OC", disabled=True, key=f"btn_send_oc_{nome_empresa_calc}")
        
    else:
        if st.button(f"Enviar {qtd_sel} itens selecionados para a Cesta de OC", type="secondary", key=f"btn_send_oc_{nome_empresa_calc}"):
            df_para_cesta = df_selecionados[df_selecionados["Compra_Sugerida"] > 0].copy()
            if not df_para_cesta.empty:
                try:
                    adicionar_itens_cesta(nome_empresa_calc, df_para_cesta)
                    st.success(f"{len(df_para_cesta)} itens de {nome_empresa_calc} enviados para a Cesta de OC (Tab 4).")
                except Exception as e:
                    st.error(f"Erro ao enviar para a cesta: {e}")
            else:
                st.warning("Nada foi enviado (nenhum item válido selecionado).")


def renderizar_painel_conjunta(df_conjunta_mesclada, state):
    """
    Renderiza a tabela CONJUNTA (mesclada) e o botão de envio inteligente.
    """
    st.info("Modo 'Conjunta': Tabela comparativa. O botão 'Enviar' divide os itens para as cestas de ALIVVIA e JCA.")
    
    # Filtros
    c_f1, c_f2 = st.columns(2)
    fornecedores = sorted(df_conjunta_mesclada["fornecedor"].fillna("").astype(str).unique().tolist())
    filtro_forn = c_f1.multiselect("Filtrar Fornecedor", fornecedores, key="filtro_forn_CONJUNTA")
    filtro_sku_text = c_f2.text_input("Buscar SKU/Parte do SKU", key="filtro_sku_CONJUNTA").strip()

    df_filtrado = df_conjunta_mesclada.copy()
    if filtro_forn:
        df_filtrado = df_filtrado[df_filtrado["fornecedor"].isin(filtro_forn)]
    if filtro_sku_text:
        df_filtrado = df_filtrado[_safe_contains_series(df_filtrado["SKU"], filtro_sku_text)]
    
    # Apenas linhas onde *alguma* compra é sugerida
    base_para_editor = df_filtrado[
        (df_filtrado["Compra (Unid) - ALIVVIA"] > 0) | (df_filtrado["Compra (Unid) - JCA"] > 0)
    ].reset_index(drop=True).copy()
    
    if "Selecionar" not in base_para_editor.columns:
        base_para_editor["Selecionar"] = False

    editor_key = "data_editor_CONJUNTA"

    edited_df = st.data_editor(
        base_para_editor,
        key=editor_key,
        use_container_width=True,
        height=600,
        column_config={
            "Selecionar": st.column_config.CheckboxColumn("Selecionar", default=False),
            "Vendas 60d - ALIVVIA": st.column_config.NumberColumn(format="%d"),
            "Vendas 60d - JCA": st.column_config.NumberColumn(format="%d"),
            "Estoque Físico - ALIVVIA": st.column_config.NumberColumn(format="%d"),
            "Estoque Físico - JCA": st.column_config.NumberColumn(format="%d"),
            "Compra (Unid) - ALIVVIA": st.column_config.NumberColumn(format="%d"),
            "Compra (Unid) - JCA": st.column_config.NumberColumn(format="%d"),
            "Compra (R$) - ALIVVIA": st.column_config.NumberColumn(format="R$ %.2f"),
            "Compra (R$) - JCA": st.column_config.NumberColumn(format="R$ %.2f"),
            "Preco": st.column_config.NumberColumn("Preço Único", format="R$ %.2f"),
        }
    )

    # Lógica de Seleção
    df_selecionados = pd.DataFrame()
    try:
        if isinstance(edited_df, pd.DataFrame) and "Selecionar" in edited_df.columns:
            df_selecionados = base_para_editor.merge(
                edited_df[edited_df["Selecionar"] == True][["SKU", "Selecionar"]],
                on="SKU",
                how="inner",
                suffixes=('', '_sel')
            ).drop(columns=['Selecionar_sel'])
    except Exception:
        df_selecionados = pd.DataFrame()

    qtd_sel = 0 if df_selecionados is None or df_selecionados.empty else len(df_selecionados)

    # Botão de Envio Inteligente
    if qtd_sel == 0:
        st.button("Enviar 0 itens selecionados para as Cestas de OC", disabled=True, key="btn_send_oc_CONJUNTA")
    else:
        if st.button(f"Enviar {qtd_sel} itens selecionados para as Cestas de OC", type="primary", key="btn_send_oc_CONJUNTA"):
            
            # Prepara o DF para ALIVVIA
            df_para_alivvia = df_selecionados.rename(
                columns={"Compra (Unid) - ALIVVIA": "Compra_Sugerida"}
            )[["SKU", "fornecedor", "Preco", "Compra_Sugerida"]].copy()
            df_para_alivvia = df_para_alivvia[df_para_alivvia["Compra_Sugerida"] > 0]
            
            # Prepara o DF para JCA
            df_para_jca = df_selecionados.rename(
                columns={"Compra (Unid) - JCA": "Compra_Sugerida"}
            )[["SKU", "fornecedor", "Preco", "Compra_Sugerida"]].copy()
            df_para_jca = df_para_jca[df_para_jca["Compra_Sugerida"] > 0]
            
            try:
                if not df_para_alivvia.empty:
                    adicionar_itens_cesta("ALIVVIA", df_para_alivvia)
                if not df_para_jca.empty:
                    adicionar_itens_cesta("JCA", df_para_jca)
                
                st.success(
                    f"Itens enviados para as Cestas de OC (Tab 4): "
                    f"{len(df_para_alivvia)} para ALIVVIA, {len(df_para_jca)} para JCA."
                )
            except Exception as e:
                st.error(f"Erro ao enviar para a cesta: {e}")


# Função principal (Render) - V10.12 (com fix V10.11)
def render_tab2(state, h, g, LT):
    st.subheader("Gerar Compra (por empresa ou conjunta) — lógica original")

    if state.catalogo_df is None or state.kits_df is None:
        st.info("Carregue o **Padrão (KITS/CAT)** no sidebar antes de usar as abas.")
        return

    empresa_selecionada = st.radio("Empresa ativa", ["ALIVVIA", "JCA", "CONJUNTA"], horizontal=True, key="empresa_ca")
    nome_estado = empresa_selecionada
    
    if st.button(f"Gerar Compra — {nome_estado}", type="primary"):
        state.compra_autom_data["force_recalc"] = True
        if nome_estado == "CONJUNTA":
             calcular_compra_para_empresa.clear()

    if nome_estado not in state.compra_autom_data or state.compra_autom_data.get("force_recalc", False):
        state.compra_autom_data["force_recalc"] = False
        
        try:
            if nome_estado == "CONJUNTA":
                # Calcula ALIVVIA (usando cache)
                df_alivvia, painel_a = calcular_compra_para_empresa("ALIVVIA", state, h, g, LT)
                df_a = df_alivvia[["SKU", "fornecedor", "Preco", "TOTAL_60d", "Estoque_Fisico", "Compra_Sugerida", "Valor_Compra_R$"]].copy()
                
                # Calcula JCA (usando cache)
                df_jca, painel_j = calcular_compra_para_empresa("JCA", state, h, g, LT)
                
                # =================================================================
                # >> INÍCIO DA CORREÇÃO (V10.12) - KeyError 'fornecedor - ALIVVIA' <<
                # =================================================================
                # O bug estava aqui. Esquecemos de selecionar 'fornecedor' e 'Preco' da JCA.
                df_j = df_jca[["SKU", "fornecedor", "Preco", "TOTAL_60d", "Estoque_Fisico", "Compra_Sugerida", "Valor_Compra_R$"]].copy()
                # =================================================================
                # >> FIM DA CORREÇÃO (V10.12) <<
                # =================================================================

                # Mescla os dois
                df_conjunta = pd.merge(
                    df_a, df_j,
                    on="SKU",
                    how="outer",
                    suffixes=(" - ALIVVIA", " - JCA")
                )
                
                df_conjunta = df_conjunta.fillna(0)
                # Lógica de merge de fornecedor/preço
                df_conjunta["fornecedor"] = np.where(df_conjunta["fornecedor - ALIVVIA"] != 0, df_conjunta["fornecedor - ALIVVIA"], df_conjunta["fornecedor - JCA"])
                df_conjunta["Preco"] = np.where(df_conjunta["Preco - ALIVVIA"] != 0, df_conjunta["Preco - ALIVVIA"], df_conjunta["Preco - JCA"])
                
                df_conjunta = df_conjunta.rename(columns={
                    "TOTAL_60d - ALIVVIA": "Vendas 60d - ALIVVIA",
                    "Estoque_Fisico - ALIVVIA": "Estoque Físico - ALIVVIA",
                    "Compra_Sugerida - ALIVVIA": "Compra (Unid) - ALIVVIA",
                    "Valor_Compra_R$ - ALIVVIA": "Compra (R$) - ALIVVIA",
                    "TOTAL_60d - JCA": "Vendas 60d - JCA",
                    "Estoque_Fisico - JCA": "Estoque Físico - JCA",
                    "Compra_Sugerida - JCA": "Compra (Unid) - JCA",
                    "Valor_Compra_R$ - JCA": "Compra (R$) - JCA",
                })
                
                cols_finais = [
                    "SKU", "fornecedor", "Preco",
                    "Vendas 60d - ALIVVIA", "Vendas 60d - JCA",
                    "Estoque Físico - ALIVVIA", "Estoque Físico - JCA",
                    "Compra (Unid) - ALIVVIA", "Compra (Unid) - JCA",
                    "Compra (R$) - ALIVVIA", "Compra (R$) - JCA"
                ]
                # Garante que as colunas existem antes de filtrar
                df_conjunta = df_conjunta[[col for col in cols_finais if col in df_conjunta.columns]].copy()
                
                state.compra_autom_data[nome_estado] = {"df": df_conjunta, "empresa": "CONJUNTA"}
            
            else:
                dados_display = state.get(nome_estado, {})
                col = st.columns(3)
                col[0].info(f"FULL: {dados_display.get('FULL', {}).get('name') or '—'}")
                col[1].info(f"Shopee/MT: {dados_display.get('VENDAS', {}).get('name') or '—'}")
                col[2].info(f"Estoque: {dados_display.get('ESTOQUE', {}).get('name') or '—'}")

                df_final, painel = calcular_compra_para_empresa(nome_estado, state, h, g, LT)
                
                state.compra_autom_data[nome_estado] = {
                    "df": df_final,
                    "painel": painel,
                    "empresa": nome_estado
                }
            
            st.success("Cálculo concluído.")

        except Exception as e:
            state.compra_autom_data[nome_estado] = {"error": str(e)}
            st.error(str(e))
            return

    # Renderização (V10.11 - Stale Cache Fix)
    if nome_estado in state.compra_autom_data and "df" in state.compra_autom_data[nome_estado]:
        data_fixa = state.compra_autom_data[nome_estado]
        
        if nome_estado == "CONJUNTA":
            df_cache_conjunta = data_fixa["df"]
            coluna_necessaria_v10_10 = "Compra (Unid) - ALIVVIA" 
            
            if coluna_necessaria_v10_10 not in df_cache_conjunta.columns:
                st.warning("Detectamos uma mudança de versão. Limpando cache de 'Compra Conjunta' e recarregando...")
                del state.compra_autom_data["CONJUNTA"]
                st.rerun()
            else:
                renderizar_painel_conjunta(df_cache_conjunta.copy(), state)
        
        else: # Se for ALIVVIA ou JCA (individual)
            renderizar_painel_individual(
                data_fixa["df"].copy(), 
                data_fixa["painel"], 
                data_fixa["empresa"],
                state
            )