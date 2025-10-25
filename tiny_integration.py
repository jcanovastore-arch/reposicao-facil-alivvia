# tiny_integration.py
# Integração Tiny ERP — Estoque confiável (dupla leitura, reconsulta de outliers, gatekeeper)
# Copie este arquivo inteiro no seu projeto.

import time
import io
import json
import random
import requests
import pandas as pd
import numpy as np
from unidecode import unidecode


# ===================== Utilidades =====================

def _norm(s: str) -> str:
    return unidecode(str(s or "")).strip().lower()

def br_to_float(x):
    """Converte '46,36' -> 46.36; '1.234,56' -> 1234.56; aceita número direto."""
    if pd.isna(x):
        return np.nan
    if isinstance(x, (int, float, np.integer, np.floating)):
        return float(x)
    s = str(x).strip().replace("\u00a0", " ").replace("R$", "").replace(" ", "")
    has_comma = "," in s
    has_dot = "." in s
    if has_comma and not has_dot:
        s = s.replace(",", ".")
    elif has_comma and has_dot:
        s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except:
        return np.nan


# ===================== Cliente Tiny com Retry/Backoff =====================

class TinyClient:
    def __init__(self, token: str, timeout=30, max_retries=3, backoff_base=0.7):
        self.token = token
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff_base = backoff_base
        self.s = requests.Session()
        self.s.headers.update({"User-Agent": "Alivvia-Reposicao/1.0"})

    def _post(self, url: str, data: dict):
        data = {**data, "token": self.token, "formato": "json"}
        for k in range(self.max_retries):
            try:
                r = self.s.post(url, data=data, timeout=self.timeout)
                # Se limite/erro de servidor, re-tenta com backoff exponencial
                if r.status_code == 429 or r.status_code >= 500:
                    time.sleep(self.backoff_base * (2 ** k))
                    continue
                return r
            except requests.RequestException:
                time.sleep(self.backoff_base * (2 ** k))
        return None

    def listar_produtos(self, pagina=1):
        url = "https://api.tiny.com.br/api2/produto.listar.php"
        r = self._post(url, {"pagina": pagina})
        if not r:
            return None, {"erro": "timeout/listar"}
        try:
            payload = r.json()
        except Exception:
            return None, {"erro": f"http {r.status_code}", "raw": r.text[:500]}
        return payload, None

    def obter_estoque_sku(self, sku: str):
        url = "https://api.tiny.com.br/api2/produto.obter.estoque.php"
        r = self._post(url, {"sku": sku})
        if not r:
            return None, {"erro": "timeout/obter_estoque"}
        try:
            payload = r.json()
        except Exception:
            return None, {"erro": f"http {r.status_code}", "raw": r.text[:500]}
        return payload, None


# ===================== Regras de Estoque =====================

def somar_estoque_geral(depositos: list) -> int:
    """
    Soma somente o depósito 'Geral' (normalizando nome), ignora desconsiderar=S,
    aceita campos saldo/saldo_disponivel/qtd. Retorna int >= 0.
    """
    total = 0.0
    if not isinstance(depositos, list):
        return 0
    for d in depositos:
        nome = _norm(d.get("nome"))
        if nome != "geral":
            continue
        descons = str(d.get("desconsiderar", "N")).strip().upper()
        if descons == "S":
            continue
        saldo = d.get("saldo", None)
        if saldo in (None, "", "None"):
            saldo = d.get("saldo_disponivel", None)
        if saldo in (None, "", "None"):
            saldo = d.get("qtd", 0)
        val = br_to_float(saldo)
        if pd.isna(val):
            val = 0.0
        total += float(val)
    return int(round(max(total, 0)))


# ===================== Leitura Confiável =====================

def _ler_once(client: TinyClient, skus_alvo: list, sleep_between=0.12):
    """
    Lê estoque/preço dos SKUs (1 a 1) com pausa padrão 120ms para evitar rate-limit.
    Retorna (DataFrame, qtd_erros).
    """
    linhas = []
    erros = 0
    for sku in skus_alvo:
        payload, err = client.obter_estoque_sku(sku)
        if err:
            erros += 1
            linhas.append({"SKU": sku, "Estoque_Fisico": 0, "Preco": 0.0, "erro": err.get("erro", "?")})
        else:
            dep = payload.get("retorno", {}).get("depositos", [])
            preco = payload.get("retorno", {}).get("produto", {}).get("preco_custo_medio", 0)
            linhas.append({
                "SKU": sku,
                "Estoque_Fisico": somar_estoque_geral(dep),
                "Preco": br_to_float(preco) or 0.0,
                "erro": ""
            })
        if sleep_between:
            time.sleep(sleep_between)
    df = pd.DataFrame(linhas)
    if not df.empty:
        df["SKU"] = df["SKU"].astype(str).str.upper().str.strip()
        df["Estoque_Fisico"] = pd.to_numeric(df["Estoque_Fisico"], errors="coerce").fillna(0).astype(int)
        df["Preco"] = pd.to_numeric(df["Preco"], errors="coerce").fillna(0.0)
    return df, erros

def _auditar_diferencas(dfA, dfB, col="Estoque_Fisico"):
    m = dfA.merge(dfB, on="SKU", how="outer", suffixes=("_A", "_B"))
    m[col+"_A"] = pd.to_numeric(m[col+"_A"], errors="coerce").fillna(0).astype(int)
    m[col+"_B"] = pd.to_numeric(m[col+"_B"], errors="coerce").fillna(0).astype(int)
    m["diff"] = (m[col+"_B"] - m[col+"_A"]).abs()
    m["pct"] = np.where(
        m[col+"_A"] > 0,
        m["diff"] / m[col+"_A"] * 100.0,
        np.where(m[col+"_B"] > 0, 100.0, 0.0)
    )
    return m

def ler_estoque_confiavel(conta: str, token: str, skus_catalogo: list,
                          delta_pct_outlier=50.0, modo_confiavel=True, ultimo_df=None):
    """
    Leitura robusta:
      - leitura A e B
      - reconsulta somente dos outliers (diferença > delta_pct_outlier)
      - fallback no ultimo_df se algo faltar
    Retorna (df[SKU,Estoque_Fisico,Preco], audit_dict)
    """
    client = TinyClient(token)
    skus = sorted({str(s).upper().strip() for s in skus_catalogo if str(s).strip()})
    if not skus:
        return pd.DataFrame(columns=["SKU","Estoque_Fisico","Preco"]), {"erro": "catalogo_vazio"}

    # leitura A
    dfA, errA = _ler_once(client, skus)

    if not modo_confiavel:
        audit = {
            "conta": conta, "modo_confiavel": False,
            "lidos": int(dfA.shape[0]), "erros": int(errA),
            "soma_estoque": int(dfA["Estoque_Fisico"].sum()),
            "ts": time.strftime("%Y-%m-%d %H:%M:%S")
        }
        return dfA[["SKU","Estoque_Fisico","Preco"]], audit

    # leitura B
    dfB, errB = _ler_once(client, skus)

    # outliers entre A e B
    comp = _auditar_diferencas(dfA, dfB, col="Estoque_Fisico")
    outliers = comp.loc[comp["pct"] > float(delta_pct_outlier), "SKU"].dropna().astype(str).tolist()

    # reconsulta só dos outliers
    dfC = pd.DataFrame(columns=["SKU","Estoque_Fisico","Preco","erro"]); errC = 0
    if outliers:
        dfC, errC = _ler_once(client, outliers, sleep_between=0.12)

    # consolidação: base = B; substitui por C; preenche faltantes com A; se ainda faltar, usa ultimo_df
    base = dfB.set_index("SKU")
    if not dfC.empty:
        for sku, row in dfC.set_index("SKU").iterrows():
            base.loc[sku, ["Estoque_Fisico","Preco"]] = [row["Estoque_Fisico"], row["Preco"]]

    a_map = dfA.set_index("SKU")
    for sku in skus:
        if sku not in base.index:
            if sku in a_map.index:
                base.loc[sku, ["Estoque_Fisico","Preco"]] = [int(a_map.loc[sku,"Estoque_Fisico"]), float(a_map.loc[sku,"Preco"])]
            elif ultimo_df is not None and sku in ultimo_df.set_index("SKU").index:
                u = ultimo_df.set_index("SKU")
                base.loc[sku, ["Estoque_Fisico","Preco"]] = [int(u.loc[sku,"Estoque_Fisico"]), float(u.loc[sku,"Preco"])]
            else:
                base.loc[sku, ["Estoque_Fisico","Preco"]] = [0, 0.0]

    base = base.reset_index()
    base["Estoque_Fisico"] = pd.to_numeric(base["Estoque_Fisico"], errors="coerce").fillna(0).astype(int)
    base["Preco"] = pd.to_numeric(base["Preco"], errors="coerce").fillna(0.0)

    audit = {
        "conta": conta,
        "modo_confiavel": True,
        "lidos_A": int(dfA.shape[0]), "erros_A": int(errA),
        "lidos_B": int(dfB.shape[0]), "erros_B": int(errB),
        "outliers_pct_limite": float(delta_pct_outlier),
        "outliers_reconsultados": int(len(outliers)),
        "erros_reconsulta": int(errC),
        "soma_estoque_final": int(base["Estoque_Fisico"].sum()),
        "ts": time.strftime("%Y-%m-%d %H:%M:%S"),
    }
    return base[["SKU","Estoque_Fisico","Preco"]], audit


# ===================== Revalidação por Amostra + Gatekeeper =====================

def _revalidar_amostra(client: TinyClient, df_base: pd.DataFrame, sample_n=50):
    """
    Reconsulta uma amostra de SKUs e mede o desvio relativo da coluna Estoque_Fisico.
    Retorna (df_amostra, erro_medio_pct).
    """
    skus = df_base["SKU"].dropna().astype(str).tolist()
    if not skus:
        return pd.DataFrame(columns=["SKU","Estoque_Fisico_base","Estoque_Fisico_check","rel_err_pct"]), 0.0

    pick = skus if len(skus) <= sample_n else random.sample(skus, sample_n)

    linhas = []
    for sku in pick:
        payload, err = client.obter_estoque_sku(sku)
        if err:
            linhas.append({"SKU": sku, "Estoque_Fisico": np.nan})
        else:
            dep = payload.get("retorno", {}).get("depositos", [])
            linhas.append({"SKU": sku, "Estoque_Fisico": somar_estoque_geral(dep)})
        time.sleep(0.12)  # respeitar limite

    df_check = pd.DataFrame(linhas)
    df_check["Estoque_Fisico"] = pd.to_numeric(df_check["Estoque_Fisico"], errors="coerce")

    base = df_base[["SKU","Estoque_Fisico"]].merge(
        df_check, on="SKU", how="left", suffixes=("_base","_check")
    )

    base["abs_diff"] = (base["Estoque_Fisico_check"] - base["Estoque_Fisico_base"]).abs()
    base["rel_err_pct"] = np.where(
        base["Estoque_Fisico_base"] > 0,
        (base["abs_diff"] / base["Estoque_Fisico_base"]) * 100.0,
        np.where(base["Estoque_Fisico_check"] > 0, 100.0, 0.0)
    )
    erro_medio = float(base["rel_err_pct"].fillna(0).mean())
    return base, erro_medio

def gatekeep_snapshot(df_novo: pd.DataFrame,
                      df_antigo: pd.DataFrame | None,
                      client: TinyClient,
                      amostra=50,
                      limite_total_pct=10.0,
                      limite_skus_pct=30.0,
                      limite_erro_amostra_pct=15.0):
    """
    Decide se aceita o snapshot novo:
      1) |Δ total físico| <= limite_total_pct (vs anterior), se houver anterior
      2) % de SKUs alterados <= limite_skus_pct (vs anterior), se houver anterior
      3) Erro médio na amostra revalidada <= limite_erro_amostra_pct
    Retorna (aceita: bool, audit_extra: dict, df_amostra: pd.DataFrame)
    """
    audit = {}

    # 1/2: comparações contra anterior (se existir)
    if df_antigo is not None and not df_antigo.empty:
        tot_old = float(pd.to_numeric(df_antigo["Estoque_Fisico"], errors="coerce").fillna(0).sum())
        tot_new = float(pd.to_numeric(df_novo["Estoque_Fisico"], errors="coerce").fillna(0).sum())
        delta_pct = 0.0 if tot_old == 0 else abs(tot_new - tot_old) / tot_old * 100.0
        audit["delta_total_pct"] = round(delta_pct, 3)

        m = df_antigo[["SKU","Estoque_Fisico"]].merge(
            df_novo[["SKU","Estoque_Fisico"]], on="SKU", how="outer", suffixes=("_old","_new")
        ).fillna(0)
        m["changed"] = (m["Estoque_Fisico_old"] != m["Estoque_Fisico_new"]).astype(int)
        pct_changed = float(m["changed"].mean() * 100.0)
        audit["pct_skus_alterados"] = round(pct_changed, 3)
    else:
        delta_pct = 0.0
        pct_changed = 0.0
        audit["delta_total_pct"] = None
        audit["pct_skus_alterados"] = None

    # 3) revalidação por amostra
    df_amostra, erro_medio = _revalidar_amostra(client, df_novo, sample_n=amostra)
    audit["erro_medio_amostra_pct"] = round(float(erro_medio), 3)

    aceita = True
    motivos = []
    if df_antigo is not None and not df_antigo.empty:
        if delta_pct > float(limite_total_pct):
            aceita = False; motivos.append(f"Δ total {delta_pct:.2f}% > {limite_total_pct:.2f}%")
        if pct_changed > float(limite_skus_pct):
            aceita = False; motivos.append(f"% SKUs alterados {pct_changed:.2f}% > {limite_skus_pct:.2f}%")
    if erro_medio > float(limite_erro_amostra_pct):
        aceita = False; motivos.append(f"erro amostra {erro_medio:.2f}% > {limite_erro_amostra_pct:.2f}%")

    audit["gate_ok"] = aceita
    audit["gate_falhas"] = motivos
    return aceita, audit, df_amostra


# ===================== UI Helper (Streamlit) =====================

def render_estoque_section(emp: str, _store_put, _store_delete, badge_ok):
    """
    Desenha o bloco 'Estoque Físico — Tiny' com:
      - Checkbox Modo Confiável
      - Botão 'Forçar sincronização com o Tiny'
      - Gatekeeper de consistência
      - Salvamento do XLSX
      - Auditoria visível
    Retorna (df, fname) quando salvar; senão (None, None).
    """
    import streamlit as st
    from datetime import datetime

    # 1) Tokens (menção direta conforme passado por você)
    TOKENS = {
        "ALIVVIA": "b3ca9c3319ac75276c03e097296e15619259cab9029a1e45b781a07553bdb25b",
        "JCA":     "352880e9498ec1a29b81a9f0ea1a946a46415f93b2aa2706634f39064b750dcd",
    }
    token = TOKENS.get(emp.upper())

    # 2) Catálogo do app (lista de SKUs permitidos)
    cat_df = st.session_state.get("catalogo_df", None)
    if cat_df is None or "sku" not in cat_df.columns:
        st.warning("Carregue o Padrão (KITS/CAT) no sidebar para habilitar o estoque via Tiny.")
        return None, None
    skus_catalogo = cat_df["sku"].dropna().astype(str).tolist()

    st.markdown("**Estoque Físico — origem: Tiny (depósito ‘Geral’ + catálogo do app)**")

    # Checkbox Modo Confiável
    confiavel = st.checkbox(
        "Modo Confiável (dupla leitura + reconsulta de outliers)",
        value=True, key=f"conf_tiny_{emp}"
    )

    # Carregar último estoque salvo da sessão (para fallback e para comparar no gate)
    def _load_last_stock_df_from_session():
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
            # normaliza colunas
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
            return df[["SKU","Estoque_Fisico","Preco"]]
        except Exception:
            return None

    # Botão
    btn = st.button(f"🔄 Forçar sincronização com o Tiny", use_container_width=True, key=f"force_tiny_{emp}")

    if not btn:
        return None, None

    if not token:
        st.error(f"Token da conta {emp} não configurado.")
        return None, None

    with st.spinner(f"Atualizando {emp} (Tiny)…"):
        ultimo_df = _load_last_stock_df_from_session()

        # Leitura confiável
        df_estoque, audit = ler_estoque_confiavel(
            conta=emp,
            token=token,
            skus_catalogo=skus_catalogo,
            delta_pct_outlier=50.0,
            modo_confiavel=confiavel,
            ultimo_df=ultimo_df
        )

        if df_estoque is None or df_estoque.empty:
            st.error("Tiny retornou vazio. Nada foi salvo.")
            return None, None

        # ===== Gatekeeper =====
        client = TinyClient(token)
        df_antigo = ultimo_df  # já carregado acima

        aceita, audit_gate, df_amostra = gatekeep_snapshot(
            df_novo=df_estoque, df_antigo=df_antigo, client=client,
            amostra=50,                  # tamanho da amostra a revalidar
            limite_total_pct=10.0,       # Δ total permitido
            limite_skus_pct=30.0,        # % SKUs alterados
            limite_erro_amostra_pct=15.0 # erro médio permitido
        )

        audit.update(audit_gate)

        if not aceita:
            st.error("⚠️ Instabilidade detectada — snapshot REPROVADO pelo gatekeeper. Mantive o arquivo anterior.")
            with st.expander("📋 Auditoria (motivos)", expanded=True):
                st.json(audit)
            return None, None

        # ===== Salvar XLSX =====
        df_final = df_estoque[["SKU","Estoque_Fisico","Preco"]].copy()
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

        ts = datetime.now().strftime("%Y%m%d_%H%M")
        fname = f"estoque_api_{emp}_{ts}.xlsx"

        _store_put(emp, "ESTOQUE", fname, bio.getvalue())
        st.markdown(badge_ok("Estoque salvo", fname), unsafe_allow_html=True)

        # Auditoria visível
        with st.expander("📋 Auditoria de sincronização (Tiny)", expanded=False):
            st.json(audit)

        return df_final, fname
