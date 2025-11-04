# mod_compra_autom.py - TAB 2 - V10.9
# - FIX: Corrige o crash "@st.cache_data" (Cannot hash argument 'state')
# - REVERTE FLUXO CONJUNTA: Volta a calcular a lista Agregada (para a Tab 3 usar)
# - Mantém Formatação (V10.7) e Fix da Cesta (V10.6)

import pandas as pd
import streamlit as st
import numpy as np
from unidecode import unidecode # Necessário para norm_sku

import logica_compra
from logica_compra import (
    Catalogo,
    aggregate_data_for_conjunta_clean, # Re-adicionado
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

# =================================================================
# >> INÍCIO DA CORREÇÃO (V10.9) - Crash do @st.cache_data <<
# =================================================================
@st.cache_data(show_spinner="Calculando Compra para _empresa_...")
def calcular_compra_para_empresa(_empresa_, _state, h, g, LT): # <- 'state' mudou para '_state'
    """
    Função cacheada que executa a lógica de cálculo para UMA empresa.
    O argumento '_state' é ignorado pelo cache.
    """
    # Lê do _state (que é o st.session_state)
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
# =================================================================
# >> FIM DA CORREÇÃO (V10.9) <<
# =================================================================


def renderizar_painel_e_tabela(df_final, painel, nome_empresa_calc, state, is_conjunta=False):
    """
    Função helper para renderizar a tabela formatada e o botão de envio.
    """
    
    # Renderização do Painel (com formatação)
    cA, cB, cC, cD = st.columns(4)
    cA.metric("Full (un)",   f"{int(painel.get('full_unid', 0)):,}".replace(",", "."))
    cB.metric("Full (R$)",   f"R$ {float(painel.get('full_valor', 0.0)):,.2f}")
    cC.metric("Físico (un)", f"{int(painel.get('fisico_unid', 0)):,}".replace(",", "."))
    cD.metric("Físico (R$)", f"R$ {float(painel.get('fisico_valor', 0.0)):,.2f}")

    # Filtros
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

    # Lógica de Seleção (V10.7)
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
    # =================================================================
    # >> INÍCIO DA CORREÇÃO (V10.9) - Lógica do Botão <<
    # =================================================================
    # Se for "CONJUNTA" (agregada), desabilita o botão e mostra o aviso.
    if is_conjunta:
        st.button("Enviar 0 itens selecionados para a Cesta de OC", disabled=True, key=f"btn_send_oc_{nome_empresa_calc}",
                  help="Use a Tab 'Alocação de Compra' para itens CONJUNTOS.")
        st.warning("⚠️ Compra Conjunta gerada! Use a aba **'📦 Alocação de Compra'** (Tab 3) para fracionar o lote e enviar para OC.")
    
    # Se for individual (ALIVVIA ou JCA)
    elif qtd_sel == 0:
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
    # =================================================================
    # >> FIM DA CORREÇÃO (V10.9) <<
    # =================================================================


# Função principal (Render)
def render_tab2(state, h, g, LT):
    st.subheader("Gerar Compra (por empresa ou conjunta) — lógica original")

    if state.catalogo_df is None or state.kits_df is None:
        st.info("Carregue o **Padrão (KITS/CAT)** no sidebar antes de usar as abas.")
        return

    empresa_selecionada = st.radio("Empresa ativa", ["ALIVVIA", "JCA", "CONJUNTA"], horizontal=True, key="empresa_ca")
    nome_estado = empresa_selecionada # ALIVVIA, JCA, ou CONJUNTA
    
    # =================================================================
    # >> INÍCIO DA CORREÇÃO (V10.9) - Revertendo Fluxo "CONJUNTA" <<
    # =================================================================
    
    # Lógica de Disparo (ou manutenção do estado)
    if st.button(f"Gerar Compra — {nome_estado}", type="primary"):
        state.compra_autom_data["force_recalc"] = True

    # Se o cálculo não existir no estado ou se for forçado, execute-o
    if nome_estado not in state.compra_autom_data or state.compra_autom_data.get("force_recalc", False):
        state.compra_autom_data["force_recalc"] = False
        
        try:
            # FLUXO CONJUNTA (Agregado - como no V10.6)
            if nome_estado == "CONJUNTA":
                st.info("Modo 'Conjunta': Calculando lista agregada para a Tab 3 (Alocação).")
                
                dfs = {}
                missing = []
                for emp in ("ALIVVIA", "JCA"):
                    dados = state.get(emp, {})
                    for k, rot in (("FULL","FULL"), ("VENDAS","Shopee/MT"), ("ESTOQUE","Estoque")):
                        slot_data = dados.get(k, {})
                        if not (slot_data.get("name") and slot_data.get("bytes")):
                            missing.append(f"{emp} {rot}")
                            continue
                        
                        raw_bytes = slot_data["bytes"]
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
                    )

                full_df, fisico_df, vendas_df = aggregate_data_for_conjunta_clean(
                    dfs["full_A"], dfs["vend_A"], dfs["fisi_A"],
                    dfs["full_J"], dfs["vend_J"], dfs["fisi_J"]
                )
                nome_empresa_calc = "CONJUNTA"
                
                cat = Catalogo(
                    catalogo_simples=state.catalogo_df.rename(columns={"sku": "component_sku"}),
                    kits_reais=state.kits_df
                )
                df_final, painel = calcular_compra(full_df, fisico_df, vendas_df, cat, h=h, g=g, LT=LT)

            
            # FLUXO INDIVIDUAL (ALIVVIA ou JCA)
            else:
                dados_display = state.get(nome_estado, {})
                col = st.columns(3)
                col[0].info(f"FULL: {dados_display.get('FULL', {}).get('name') or '—'}")
                col[1].info(f"Shopee/MT: {dados_display.get('VENDAS', {}).get('name') or '—'}")
                col[2].info(f"Estoque: {dados_display.get('ESTOQUE', {}).get('name') or '—'}")

                # Passa o 'state' completo, mas a função cacheada o ignora (com _state)
                df_final, painel = calcular_compra_para_empresa(nome_estado, state, h, g, LT)
                nome_empresa_calc = nome_estado
            
            # Salva o resultado no estado (caching)
            state.compra_autom_data[nome_estado] = {
                "df": df_final,
                "painel": painel,
                "empresa": nome_empresa_calc
            }
            st.success("Cálculo concluído.")

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
            state,
            is_conjunta=(nome_estado == "CONJUNTA")
        )
    # =================================================================
    # >> FIM DA CORREÇÃO (V10.9) <<
    # =================================================================