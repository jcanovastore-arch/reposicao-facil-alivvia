# mod_compra_autom.py - MÓDULO DA TAB 2 - FIX V9.0
# - Corrige typo na métrica ("painl" -> "painel")
# - Torna o st.data_editor à prova de crash (usa retorno do widget e fallback p/ session_state)

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

# ------------------------- Helpers internos -------------------------

def _require_cols(df: pd.DataFrame, cols: list[str], ctx: str):
    """Garante que colunas existam antes de continuar (evita KeyError mais adiante)."""
    faltando = [c for c in cols if c not in df.columns]
    if faltando:
        raise RuntimeError(f"[{ctx}] Faltam colunas: {', '.join(faltando)}")

def _safe_contains_series(series: pd.Series, text: str) -> pd.Series:
    """contains case-insensitive com fallback quando houver NaN."""
    try:
        return series.fillna("").astype(str).str.contains(text, case=False, na=False)
    except Exception:
        return pd.Series([False] * len(series), index=series.index)

# ------------------------- Render Tab 2 -------------------------

def render_tab2(state, h, g, LT):
    """Renderiza toda a aba 'Compra Automática'."""
    st.subheader("Gerar Compra (por empresa ou conjunta) — lógica original")

    if state.catalogo_df is None or state.kits_df is None:
        st.info("Carregue o **Padrão (KITS/CAT)** no sidebar antes de usar as abas.")
        return

    # 1) Seleção de escopo
    empresa_selecionada = st.radio(
        "Empresa ativa", ["ALIVVIA", "JCA", "CONJUNTA"],
        horizontal=True, key="empresa_ca"
    )
    nome_estado = empresa_selecionada

    # 1.1) Painel de arquivos carregados (exceto Conjunta)
    if nome_estado == "CONJUNTA":
        st.info("Arquivos agregados prontos para o cálculo Conjunto.")
    else:
        dados_display = state[nome_estado]
        col = st.columns(3)
        col[0].info(f"FULL: {dados_display['FULL']['name'] or '—'}")
        col[1].info(f"Shopee/MT: {dados_display['VENDAS']['name'] or '—'}")
        col[2].info(f"Estoque: {dados_display['ESTOQUE']['name'] or '—'}")

    # 2) Disparo do cálculo
    if st.button(f"Gerar Compra — {nome_estado}", type="primary"):
        state.compra_autom_data["force_recalc"] = True

    # 2.1) Executa cálculo se necessário
    if nome_estado not in state.compra_autom_data or state.compra_autom_data.get("force_recalc", False):

        state.compra_autom_data["force_recalc"] = False

        try:
            # Catálogo/kits (renomeia para o que a lógica espera)
            cat = Catalogo(
                catalogo_simples=state.catalogo_df.rename(columns={"sku": "component_sku"}),
                kits_reais=state.kits_df
            )

            # 2.2) Montagem dos DataFrames base por escopo
            if nome_estado == "CONJUNTA":
                dfs = {}
                missing = []

                for emp in ("ALIVVIA", "JCA"):
                    dados = state[emp]
                    for k, rot in (("FULL", "FULL"), ("VENDAS", "Shopee/MT"), ("ESTOQUE", "Estoque")):
                        if not (dados[k]["name"] and dados[k]["bytes"]):
                            missing.append(f"{emp} {rot}")
                            continue

                        raw = load_any_table_from_bytes(dados[k]["name"], dados[k]["bytes"])
                        tipo = mapear_tipo(raw)

                        if tipo == "FULL":
                            dfs[f"full_{emp[0]}"] = mapear_colunas(raw, tipo)
                        elif tipo == "VENDAS":
                            dfs[f"vend_{emp[0]}"] = mapear_colunas(raw, tipo)
                        elif tipo == "FISICO":
                            dfs[f"fisi_{emp[0]}"] = mapear_colunas(raw, tipo)
                        else:
                            raise RuntimeError(f"Arquivo {rot} de {emp} com formato incorreto: {tipo}.")

                if missing:
                    raise RuntimeError(
                        "Arquivos necessários para Compra Conjunta estão ausentes: "
                        + ", ".join(missing)
                        + ". Recarregue todos na aba 'Dados das Empresas'."
                    )

                full_df, fisico_df, vendas_df = aggregate_data_for_conjunta_clean(
                    dfs["full_A"], dfs["vend_A"], dfs["fisi_A"],
                    dfs["full_J"], dfs["vend_J"], dfs["fisi_J"]
                )
                nome_empresa_calc = "CONJUNTA"

            else:
                dados = state[nome_estado]

                # Verificação de presença
                for k, rot in (("FULL", "FULL"), ("VENDAS", "Shopee/MT"), ("ESTOQUE", "Estoque")):
                    if not (dados[k]["name"] and dados[k]["bytes"]):
                        raise RuntimeError(
                            f"Arquivo '{rot}' não foi salvo para {nome_estado}. "
                            f"Vá em **Dados das Empresas** e salve."
                        )

                # Carrega brutos
                full_raw   = load_any_table_from_bytes(dados["FULL"]["name"],    dados["FULL"]["bytes"])
                vendas_raw = load_any_table_from_bytes(dados["VENDAS"]["name"],  dados["VENDAS"]["bytes"])
                fisico_raw = load_any_table_from_bytes(dados["ESTOQUE"]["name"], dados["ESTOQUE"]["bytes"])

                # Checa tipos
                t_full = mapear_tipo(full_raw)
                t_v    = mapear_tipo(vendas_raw)
                t_f    = mapear_tipo(fisico_raw)

                if t_full != "FULL" or t_v != "VENDAS" or t_f != "FISICO":
                    raise RuntimeError("Um ou mais arquivos (FULL/VENDAS/FISICO) estão com formato incorreto.")

                # Mapeia colunas
                full_df   = mapear_colunas(full_raw, t_full)
                vendas_df = mapear_colunas(vendas_raw, t_v)
                fisico_df = mapear_colunas(fisico_raw, t_f)
                nome_empresa_calc = nome_estado

            # 2.3) CÁLCULO PRINCIPAL
            df_final, painel = calcular_compra(
                full_df, fisico_df, vendas_df, cat, h=h, g=g, LT=LT
            )

            # Garante colunas usadas abaixo
            for col_req in ("SKU", "fornecedor", "Compra_Sugerida"):
                if col_req not in df_final.columns:
                    raise RuntimeError(f"[Pós-cálculo] Coluna ausente: {col_req}")

            # Coluna de seleção para o editor
            if "Selecionar" not in df_final.columns:
                df_final["Selecionar"] = False

            # SALVA NO ESTADO (CACHE DA ABA)
            state.compra_autom_data[nome_estado] = {
                "df": df_final,
                "painel": painel,
                "empresa": nome_empresa_calc,
            }

            st.success("Cálculo concluído. Selecione itens abaixo para Ordem de Compra.")

        except Exception as e:
            state.compra_autom_data[nome_estado] = {"error": str(e)}
            st.error(str(e))
            return

    # 3) Renderização de resultados
    if nome_estado in state.compra_autom_data and "df" in state.compra_autom_data[nome_estado]:
        data_fixa = state.compra_autom_data[nome_estado]
        df_final = data_fixa["df"].copy()
        painel   = data_fixa["painel"]
        nome_empresa_calc = data_fixa["empresa"]

        if nome_empresa_calc == "CONJUNTA":
            st.warning("⚠️ Compra Conjunta gerada! Use a aba **'📦 Alocação de Compra'** para fracionar o lote sugerido.")

        # 3.1) Métricas do painel
        cA, cB, cC, cD = st.columns(4)
        cA.metric("Full (un)",   f"{int(painel.get('full_unid', 0)):,}".replace(",", "."))
        cB.metric("Full (R$)",   f"R$ {float(painel.get('full_valor', 0.0)):,.2f}")
        cC.metric("Físico (un)", f"{int(painel.get('fisico_unid', 0)):,}".replace(",", "."))
        # CORRIGIDO: 'painel' (antes havia 'painl')
        cD.metric("Físico (R$)", f"R$ {float(painel.get('fisico_valor', 0.0)):,.2f}")

        # 3.2) Filtros
        c_f1, c_f2 = st.columns(2)

        _require_cols(df_final, ["fornecedor", "SKU", "Compra_Sugerida"], "Filtros/Render")
        fornecedores = sorted(df_final["fornecedor"].fillna("").astype(str).unique().tolist())
        filtro_forn = c_f1.multiselect("Filtrar Fornecedor", fornecedores)

        filtro_sku_text = c_f2.text_input(
            "Buscar SKU/Parte do SKU", key=f"filtro_sku_{nome_estado}"
        ).strip()

        df_filtrado = df_final.copy()
        if filtro_forn:
            df_filtrado = df_filtrado[df_filtrado["fornecedor"].isin(filtro_forn)]
        if filtro_sku_text:
            df_filtrado = df_filtrado[_safe_contains_series(df_filtrado["SKU"], filtro_sku_text)]

        # 3.3) Grade com seleção
        base_para_editor = df_filtrado[df_filtrado["Compra_Sugerida"] > 0].reset_index(drop=True).copy()
        if "Selecionar" not in base_para_editor.columns:
            base_para_editor["Selecionar"] = False

        editor_key = f"data_editor_{nome_estado}"

        # Preferir o retorno do widget (novo comportamento do Streamlit)
        edited_df = st.data_editor(
            base_para_editor,
            key=editor_key,
            use_container_width=True,
            height=500,
            column_config={
                "Selecionar": st.column_config.CheckboxColumn("Selecionar", default=False)
            },
        )

        # 3.3.1) Determina os selecionados
        df_selecionados = pd.DataFrame()

        try:
            if isinstance(edited_df, pd.DataFrame) and "Selecionar" in edited_df.columns:
                # Caminho padrão (moderno): o widget retorna o DF já editado
                df_selecionados = edited_df[edited_df["Selecionar"] == True].copy()
            else:
                # Fallback: comportamento antigo via session_state[editor_key]['edited_rows']
                raw_state = state.get(editor_key, {})
                df_base = base_para_editor.copy()
                if isinstance(raw_state, dict) and "edited_rows" in raw_state:
                    selecao_editada = pd.Series([False] * len(df_base), index=df_base.index)
                    for idx, row_data in raw_state["edited_rows"].items():
                        if isinstance(row_data, dict) and "Selecionar" in row_data:
                            selecao_editada.loc[idx] = bool(row_data["Selecionar"])
                    df_base["Selecionar"] = selecao_editada.combine_first(df_base["Selecionar"])
                    df_selecionados = df_base[df_base["Selecionar"] == True].copy()
                else:
                    df_selecionados = pd.DataFrame()
        except Exception:
            df_selecionados = pd.DataFrame()

        # 3.4) Botão de envio p/ Cesta de OC
        qtd_sel = 0 if df_selecionados is None or df_selecionados.empty else len(df_selecionados)
        if qtd_sel == 0:
            st.button("Enviar 0 itens selecionados para a Cesta de OC", disabled=True)
        else:
            if st.button(f"Enviar {qtd_sel} itens selecionados para a Cesta de OC", type="secondary"):
                # Apenas itens com Compra_Sugerida > 0
                df_selecionados = df_selecionados[df_selecionados["Compra_Sugerida"] > 0].copy()
                if not df_selecionados.empty:
                    df_selecionados["Empresa"] = nome_empresa_calc
                    if state.get("oc_cesta") is None or state.oc_cesta.empty:
                        state.oc_cesta = df_selecionados
                    else:
                        # Mantém itens de outras empresas e concatena os novos dessa
                        cesta_outros = state.oc_cesta[state.oc_cesta["Empresa"] != nome_empresa_calc].copy()
                        state.oc_cesta = pd.concat([cesta_outros, df_selecionados], ignore_index=True)

                    st.success(
                        f"Itens de {nome_empresa_calc} enviados para a Cesta de OC. "
                        f"Total na Cesta: {len(state.oc_cesta)} itens."
                    )
                    st.dataframe(state.oc_cesta, use_container_width=True)
                else:
                    st.warning("Nada foi enviado (nenhum item válido selecionado).")

        # 3.5) Export
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
