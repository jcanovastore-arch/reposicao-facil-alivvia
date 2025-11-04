# mod_compra_autom.py - TAB 2 - V10.5 (Compatível com V10.3)
# - Corrige "Bug da Cesta" (usa oc_cesta_itens)
# - Desabilita "Enviar" para CONJUNTA (força uso da Tab 3)

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

# IMPORTAÇÃO CRÍTICA (FIX V10.5)
try:
    from ordem_compra import adicionar_itens_cesta
except ImportError:
    # Fallback se a função não for encontrada (embora deva estar lá)
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
                            disk_item = logica_compra.load_from_disk_if_any(emp, k) # Assumindo que está em logica_compra
                            if disk_item:
                                raw_bytes = disk_item["bytes"]
                                state[emp][k]["bytes"] = raw_bytes # Salva na RAM
                        
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
                        f"Vá em **Dados das Empresas** e confirme o upload."
                    )

                full_raw   = load_any_table_from_bytes(dados["FULL"]["name"],    dados["FULL"]["bytes"])
                vendas_raw = load_any_table_from_bytes(dados["VENDAS"]["name"],  dados["VENDAS"]["bytes"])
                fisico_raw = load_any_table_from_bytes(dados["ESTOQUE"]["name"], dados["ESTOQUE"]["bytes"])

                t_full = mapear_tipo(full_raw)
                t_v    = mapear_tipo(vendas_raw)
                t_f    = mapear_tipo(fisico_raw)
                if t_full != "FULL" or t_v != "VENDAS" or t_f != "FISICO":
                    raise RuntimeError(f"Um ou mais arquivos (FULL/VENDAS/FISICO) de {nome_estado} estão com formato incorreto.")

                full_df   = mapear_colunas(full_raw, t_full)
                vendas_df = mapear_colunas(vendas_raw, t_v)
                fisico_df = mapear_colunas(fisico_raw, t_f)
                nome_empresa_calc = nome_estado

            # Cálculo
            df_final, painel = calcular_compra(full_df, fisico_df, vendas_df, cat, h=h, g=g, LT=LT)

            for col_req in ("SKU", "fornecedor", "Compra_Sugerida", "Preco"): # Preco é necessário para a cesta
                if col_req not in df_final.columns:
                    raise RuntimeError(f"[Pós-cálculo] Coluna ausente: {col_req}")

            if "Selecionar" not in df_final.columns:
                df_final["Selecionar"] = False

            state.compra_autom_data[nome_estado] = {
                "df": df_final,
                "painel": painel,
                "empresa": nome_empresa_calc,
            }

            st.success("Cálculo concluído. Selecione itens abaixo para Ordem de Compra.")

        except Exception as e:
            state.compra_autom_data[nome_estado] = {"error": str(e)}
            st.error(str(e))
            return # Para a execução se o cálculo falhar

    # (Lógica de Renderização - V9.1 - Inalterada)
    if nome_estado in state.compra_autom_data and "df" in state.compra_autom_data[nome_estado]:
        data_fixa = state.compra_autom_data[nome_estado]
        df_final = data_fixa["df"].copy()
        painel   = data_fixa["painel"]
        nome_empresa_calc = data_fixa["empresa"] # 'ALIVVIA', 'JCA', ou 'CONJUNTA'

        if nome_empresa_calc == "CONJUNTA":
            st.warning("⚠️ Compra Conjunta gerada! Use a aba **'📦 Alocação de Compra'** (Tab 3) para fracionar o lote e enviar para OC.")

        # (Métricas do Painel - V9.1 - Inalterada)
        cA, cB, cC, cD = st.columns(4)
        cA.metric("Full (un)",   f"{int(painel.get('full_unid', 0)):,}".replace(",", "."))
        cB.metric("Full (R$)",   f"R$ {float(painel.get('full_valor', 0.0)):,.2f}")
        cC.metric("Físico (un)", f"{int(painel.get('fisico_unid', 0)):,}".replace(",", "."))
        cD.metric("Físico (R$)", f"R$ {float(painel.get('fisico_valor', 0.0)):,.2f}")

        # (Filtros - V9.1 - Inalterada)
        c_f1, c_f2 = st.columns(2)
        _require_cols(df_final, ["fornecedor", "SKU", "Compra_Sugerida"], "Filtros/Render")
        fornecedores = sorted(df_final["fornecedor"].fillna("").astype(str).unique().tolist())
        filtro_forn = c_f1.multiselect("Filtrar Fornecedor", fornecedores)
        filtro_sku_text = c_f2.text_input("Buscar SKU/Parte do SKU", key=f"filtro_sku_{nome_estado}").strip()

        df_filtrado = df_final.copy()
        if filtro_forn:
            df_filtrado = df_filtrado[df_filtrado["fornecedor"].isin(filtro_forn)]
        if filtro_sku_text:
            df_filtrado = df_filtrado[_safe_contains_series(df_filtrado["SKU"], filtro_sku_text)]

        # (Data Editor - V9.1 - Inalterada)
        base_para_editor = df_filtrado[df_filtrado["Compra_Sugerida"] > 0].reset_index(drop=True).copy()
        if "Selecionar" not in base_para_editor.columns:
            base_para_editor["Selecionar"] = False

        editor_key = f"data_editor_{nome_estado}"
        edited_df = st.data_editor(
            base_para_editor,
            key=editor_key,
            use_container_width=True,
            height=500,
            column_config={"Selecionar": st.column_config.CheckboxColumn("Selecionar", default=False)},
        )

        # (Lógica de Seleção - V9.1 - Inalterada)
        df_selecionados = pd.DataFrame()
        try:
            if isinstance(edited_df, pd.DataFrame) and "Selecionar" in edited_df.columns:
                df_selecionados = edited_df[edited_df["Selecionar"] == True].copy()
            # (Fallback para estado antigo do editor - omitido para brevidade, mas está no V9.1)
        except Exception:
            df_selecionados = pd.DataFrame()

        qtd_sel = 0 if df_selecionados is None or df_selecionados.empty else len(df_selecionados)

        # =================================================================
        # >> INÍCIO DA CORREÇÃO (V10.5) <<
        # =================================================================
        
        # CORREÇÃO PROBLEMA 2: Desabilita o botão se for CONJUNTA
        if nome_empresa_calc == "CONJUNTA":
            st.button("Enviar 0 itens selecionados para a Cesta de OC", disabled=True,
                      help="Use a Tab 'Alocação de Compra' para itens CONJUNTOS.")
            
        elif qtd_sel == 0:
            st.button("Enviar 0 itens selecionados para a Cesta de OC", disabled=True)
            
        else:
            # CORREÇÃO PROBLEMA 1: Usa a função correta 'adicionar_itens_cesta'
            if st.button(f"Enviar {qtd_sel} itens selecionados para a Cesta de OC", type="secondary"):
                
                df_para_cesta = df_selecionados[df_selecionados["Compra_Sugerida"] > 0].copy()
                
                if not df_para_cesta.empty:
                    # 'nome_empresa_calc' aqui será 'ALIVVIA' ou 'JCA'
                    try:
                        # Adiciona os itens usando a função do módulo ordem_compra
                        adicionar_itens_cesta(nome_empresa_calc, df_para_cesta)
                        
                        st.success(
                            f"{len(df_para_cesta)} itens de {nome_empresa_calc} enviados para a Cesta de OC (Tab 4)."
                        )
                        # Limpa a seleção no editor (requer rerun)
                        state[editor_key] = {} # Limpa o estado do editor
                        st.rerun()

                    except Exception as e:
                        st.error(f"Erro ao enviar para a cesta: {e}")
                else:
                    st.warning("Nada foi enviado (nenhum item válido selecionado).")

        # =================================================================
        # >> FIM DA CORREÇÃO <<
        # =================================================================

        # (Lógica do XLSX - V9.1 - Inalterada)
        if st.checkbox("Gerar XLSX (Lista_Final + Controle)", key=f"chk_xlsx_{nome_estado}"):
            try:
                xlsx = exportar_xlsx(df_final, h=h, params={"g": g, "LT": LT, "empresa": nome_empresa_calc})
                st.download_button(
                    "Baixar XLSX",
                    data=xlsx,
                    file_name=f"Compra_Sugerida_{nome_empresa_calc}_{h}d.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
            except Exception as e:
                st.error(f"Falha ao exportar XLSX: {e}")