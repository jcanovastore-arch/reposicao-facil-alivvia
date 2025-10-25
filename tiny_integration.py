# === UI helper: seção de Estoque (com Modo Confiável) ===
def render_estoque_section(emp: str, _store_put, _store_delete, badge_ok):
    """
    Desenha o bloco 'Estoque Físico — Tiny' com:
      - Checkbox Modo Confiável (dupla leitura + reconsulta de outliers)
      - Botão 'Forçar sincronização com o Tiny'
      - Salvamento do XLSX no cofre do app
      - Resumo de auditoria em tela
    """
    import streamlit as st
    import io
    import pandas as pd
    from datetime import datetime

    # ==== 1) Tokens das contas ====
    TOKENS = {
        "ALIVVIA": "b3ca9c3319ac75276c03e097296e15619259cab9029a1e45b781a07553bdb25b",
        "JCA":     "352880e9498ec1a29b81a9f0ea1a946a46415f93b2aa2706634f39064b750dcd",
    }
    token = TOKENS.get(emp.upper())

    # ==== 2) Catálogo (lista de SKUs permitidos) ====
    cat_df = st.session_state.get("catalogo_df", None)
    if cat_df is None or "sku" not in cat_df.columns:
        st.warning("Carregue o Padrão (KITS/CAT) no sidebar para habilitar o estoque via Tiny.")
        return None, None
    skus_catalogo = cat_df["sku"].dropna().astype(str).tolist()

    st.markdown("**Estoque Físico — origem: Tiny (depósito ‘Geral’ + catálogo do app)**")

    # === checkbox do modo confiável ===
    confiavel = st.checkbox(
        "Modo Confiável (dupla leitura + reconsulta de outliers)",
        value=True, key=f"conf_tiny_{emp}"
    )

    # ==== helper para ler o último arquivo salvo (fallback por SKU) ====
    def _load_last_stock_df_from_session() -> pd.DataFrame | None:
        try:
            it = st.session_state[emp]["ESTOQUE"]
            if not (it and it.get("bytes")):
                return None
            name = (it.get("name") or "").lower()
            bio = io.BytesIO(it["bytes"])
            if name.endswith(".csv"):
                df = pd.read_csv(bio, dtype=str, keep_default_na=False)
            else:
                df = pd.read_excel(bio, dtype=str, keep_default_na=False)
            # normaliza nomes esperados
            cols = {c.lower(): c for c in df.columns}
            m = {}
            for alvo, cands in {
                "SKU": ["sku", "codigo", "codigo_sku"],
                "Estoque_Fisico": ["estoque_fisico", "estoque", "quantidade", "qtd"],
                "Preco": ["preco", "preco_medio", "preco_compra", "custo", "custo_medio"],
            }.items():
                for c in cands:
                    if c in cols:
                        m[cols[c]] = alvo
                        break
            df = df.rename(columns=m)
            if "SKU" not in df.columns:
                return None
            return df[["SKU", "Estoque_Fisico", "Preco"]]
        except Exception:
            return None

    # ==== ação do botão ====
    btn = st.button(f"🔄 Forçar sincronização com o Tiny", use_container_width=True, key=f"force_tiny_{emp}")

    if btn:
        if not token:
            st.error(f"Token da conta {emp} não configurado.")
            return None, None

        with st.spinner(f"Atualizando {emp} (Tiny)…"):
            # ultimo_df para fallback de SKUs problemáticos
            ultimo_df = _load_last_stock_df_from_session()

            # leitura robusta (ou simples se o checkbox estiver off)
            df_estoque, audit = ler_estoque_confiavel(
                conta=emp,
                token=token,
                skus_catalogo=skus_catalogo,
                delta_pct_outlier=50.0,
                modo_confiavel=confiavel,
                ultimo_df=ultimo_df
            )

            # monta XLSX (3 colunas fixas)
            if df_estoque is None or df_estoque.empty:
                st.error("Tiny retornou vazio. Nada foi salvo.")
                return None, None

            df_final = df_estoque[["SKU", "Estoque_Fisico", "Preco"]].copy()
            bio = io.BytesIO()
            with pd.ExcelWriter(bio, engine="xlsxwriter") as w:
                df_final.to_excel(w, sheet_name="Estoque", index=False)
                ws = w.sheets["Estoque"]
                for i, col in enumerate(df_final.columns):
                    width = max(12, int(df_final[col].astype(str).map(len).max()) + 2)
                    ws.set_column(i, i, min(width, 40))
                ws.freeze_panes(1, 0)
                ws.autofilter(0, 0, len(df_final), len(df_final.columns) - 1)
            bio.seek(0)

            # nome do arquivo salvo no cofre
            ts = datetime.now().strftime("%Y%m%d_%H%M")
            fname = f"estoque_api_{emp}_{ts}.xlsx"

            # salva no cofre do app (como antes)
            _store_put(emp, "ESTOQUE", fname, bio.getvalue())

            # badge verde
            st.markdown(badge_ok("Estoque salvo", fname), unsafe_allow_html=True)

            # resumo de auditoria
            with st.expander("📋 Auditoria de sincronização (Tiny)", expanded=False):
                st.json(audit)

            return df_final, fname

    # sem clique, não retorna nada
    return None, None
