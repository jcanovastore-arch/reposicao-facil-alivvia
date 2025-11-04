# mod_compra_autom.py - TAB 2 - V10.7
# - Corrige SyntaxError (adiciona norm_sku)
# - Corrige Bug da Cesta (mantém V10.6 sem st.rerun)
# - NOVO FLUXO CONJUNTA: Mostra as 2 empresas separadamente
# - ADICIONA FORMATAÇÃO de números e R$

import pandas as pd
import streamlit as st
import numpy as np
from unidecode import unidecode # Necessário para norm_sku

import logica_compra
from logica_compra import (
    Catalogo,
    aggregate_data_for_conjunta_clean, # Esta função não será mais usada aqui
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

# =================================================================
# >> INÍCIO DA CORREÇÃO (V10.7) - SyntaxError <<
# =================================================================
def norm_sku(x: str) -> str:
    """Copia da função que estava faltando e causando o SyntaxError."""
    if pd.isna(x): return ""
    return unidecode(str(x)).strip().upper()
# =================================================================
# >> FIM DA CORREÇÃO <<
# =================================================================

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


@st.cache_data(show_spinner="Calculando Compra para _empresa_...")
def calcular_compra_para_empresa(_empresa_, state, h, g, LT):
    """
    Função cacheada que executa a lógica de cálculo para UMA empresa.
    Usada pelo novo fluxo "CONJUNTA".
    """
    dados = state.get(_empresa_, {})
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
        catalogo_simples=state.catalogo_df.rename(columns={"sku": "component_sku"}),
        kits_reais=state.kits_df
    )
    
    df_final, painel = calcular_compra(full_df, fisico_df, vendas_df, cat, h=h, g=g, LT=LT)
    
    for col_req in ("SKU", "fornecedor", "Compra_Sugerida", "Preco"):
        if col_req not in df_final.columns:
            raise RuntimeError(f"[Pós-cálculo] Coluna ausente: {col_req}")

    if "Selecionar" not in df_final.columns:
        df_final["Selecionar"] = False
    
    return df_final, painel


def renderizar_painel_e_tabela(df_final, painel, nome_empresa_calc, state):
    """
    Função helper para renderizar a tabela formatada e o botão de envio.
    Usada pelo novo fluxo "CONJUNTA" e pelo fluxo individual.
    """
    
    # =================================================================
    # >> INÍCIO DA CORREÇÃO (V10.7) - Formatação <<
    # =================================================================
    # Renderização do Painel (com formatação)
    cA, cB, cC, cD = st.columns(4)
    cA.metric("Full (un)",   f"{int(painel.get('full_unid', 0)):,}".replace(",", "."))
    cB.metric("Full (R$)",   f"R$ {float(painel.get('full_valor', 0.0)):,.2f}")
    cC.metric("Físico (un)", f"{int(painel.get('fisico_unid', 0)):,}".replace(",", "."))
    cD.metric("Físico (R$)", f"R$ {float(painel.get('fisico_valor', 0.0)):,.2f}")

    # (Filtros - V9.1 - Inalterada)
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
    
    # Colunas (como pedido pelo usuário)
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
    # =================================================================
    # >> FIM DA CORREÇÃO (V10.7) - Formatação <<
    # =================================================================

    # (Lógica de Seleção - V9.1 - Inalterada)
    df_selecionados = pd.DataFrame()
    try:
        if isinstance(edited_df, pd.DataFrame) and "Selecionar" in edited_df.columns:
            # O `edited_df` retorna o DF completo, precisamos mesclar com o original para obter colunas ocultas
            df_selecionados = base_para_editor.merge(
                edited_df[edited_df["Selecionar"] == True][["SKU", "Selecionar"]],
                on="SKU",
                how="inner",
                suffixes=('', '_sel')
            ).drop(columns=['Selecionar_sel'])
            
    except Exception:
        df_selecionados = pd.DataFrame()

    qtd_sel = 0 if df_selecionados is None or df_selecionados.empty else len(df_selecionados)

    # Lógica do Botão (V10.6 - Sem st.rerun)
    if qtd_sel == 0:
        st.button("Enviar 0 itens selecionados para a Cesta de OC", disabled=True, key=f"btn_send_oc_{nome_empresa_calc}")
        
    else:
        if st.button(f"Enviar {qtd_sel} itens selecionados para a Cesta de OC", type="secondary", key=f"btn_send_oc_{nome_empresa_calc}"):
            
            df_para_cesta = df_selecionados[df_selecionados["Compra_Sugerida"] > 0].copy()
            
            if not df_para_cesta.empty:
                try:
                    adicionar_itens_cesta(nome_empresa_calc, df_para_cesta)
                    st.success(f"{len(df_para_cesta)} itens de {nome_empresa_calc} enviados para a Cesta de OC (Tab 4).")
                    # NÃO USAR ST.RERUN()
                except Exception as e:
                    st.error(f"Erro ao enviar para a cesta: {e}")
            else:
                st.warning("Nada foi enviado (nenhum item válido selecionado).")


# Função principal (Render)
def render_tab2(state, h, g, LT):
    st.subheader("Gerar Compra (por empresa ou conjunta) — lógica original")

    if state.catalogo_df is None or state.kits_df is None:
        st.info("Carregue o **Padrão (KITS/CAT)** no sidebar antes de usar as abas.")
        return

    empresa_selecionada = st.radio("Empresa ativa", ["ALIVVIA", "JCA", "CONJUNTA"], horizontal=True, key="empresa_ca")
    
    # =================================================================
    # >> INÍCIO DA CORREÇÃO (V10.7) - Novo Fluxo "CONJUNTA" <<
    # =================================================================
    
    if empresa_selecionada == "CONJUNTA":
        st.info("Modo 'Conjunta': Calculando e exibindo ALIVVIA e JCA separadamente.")
        
        try:
            # Calcula para ALIVVIA
            with st.container():
                st.markdown("### 1. Cálculo: ALIVVIA")
                df_alivvia, painel_alivvia = calcular_compra_para_empresa("ALIVVIA", state, h, g, LT)
                renderizar_painel_e_tabela(df_alivvia, painel_alivvia, "ALIVVIA", state)
            
            st.divider()
            
            # Calcula para JCA
            with st.container():
                st.markdown("### 2. Cálculo: JCA")
                df_jca, painel_jca = calcular_compra_para_empresa("JCA", state, h, g, LT)
                renderizar_painel_e_tabela(df_jca, painel_jca, "JCA", state)

        except Exception as e:
            st.error(str(e))

    else:
        # Fluxo original para ALIVVIA ou JCA individualmente
        nome_estado = empresa_selecionada
        dados_display = state.get(nome_estado, {})
        col = st.columns(3)
        col[0].info(f"FULL: {dados_display.get('FULL', {}).get('name') or '—'}")
        col[1].info(f"Shopee/MT: {dados_display.get('VENDAS', {}).get('name') or '—'}")
        col[2].info(f"Estoque: {dados_display.get('ESTOQUE', {}).get('name') or '—'}")
        
        if st.button(f"Gerar Compra — {nome_estado}", type="primary"):
            state.compra_autom_data["force_recalc"] = True

        if nome_estado not in state.compra_autom_data or state.compra_autom_data.get("force_recalc", False):
            state.compra_autom_data["force_recalc"] = False
            try:
                df_final, painel = calcular_compra_para_empresa(nome_estado, state, h, g, LT)
                
                # Salva no estado (caching)
                state.compra_autom_data[nome_estado] = {
                    "df": df_final,
                    "painel": painel,
                    "empresa": nome_estado
                }
                st.success("Cálculo concluído. Selecione itens abaixo para Ordem de Compra.")

            except Exception as e:
                state.compra_autom_data[nome_estado] = {"error": str(e)}
                st.error(str(e))
                return

        # Renderização de resultados (usando o estado salvo)
        if nome_estado in state.compra_autom_data and "df" in state.compra_autom_data[nome_estado]:
            data_fixa = state.compra_autom_data[nome_estado]
            renderizar_painel_e_tabela(
                data_fixa["df"].copy(), 
                data_fixa["painel"], 
                data_fixa["empresa"],
                state
            )

    # =================================================================
    # >> FIM DA CORREÇÃO (V10.7) <<
    # =================================================================