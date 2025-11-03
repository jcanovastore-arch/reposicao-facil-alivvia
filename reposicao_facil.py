# reposicao_facil.py - CÓDIGO FINAL DE ESTABILIDADE V9.2
# Implementa a persistência síncrona + persistência em DISCO (.uploads) para o upload.
# Após F5/refresh, os arquivos são restaurados automaticamente a partir de .uploads/.

import datetime as dt
import pandas as pd
import streamlit as st
from io import BytesIO
from pathlib import Path
import json, hashlib
from typing import Optional

# ========= MÓDULOS DO PROJETO =========
import logica_compra
import mod_compra_autom
import mod_alocacao

from logica_compra import (
    Catalogo,
    baixar_xlsx_do_sheets,
    baixar_xlsx_por_link_google,
    load_any_table_from_bytes,
    mapear_tipo,
    mapear_colunas,
    calcular as calcular_compra,
    DEFAULT_SHEET_ID,
)

# MÓDULOS DE ORDEM DE COMPRA (SQLITE)
try:
    import ordem_compra
    import gerenciador_oc
except ImportError:
    ordem_compra = None
    gerenciador_oc = None

VERSION = "v9.2 - PERSISTÊNCIA SÍNCRONA FINAL + DISCO"

# ===================== CONFIG =====================
st.set_page_config(page_title="Reposição Logística — Alivvia", layout="wide")
DEFAULT_SHEET_LINK = (
    "https://docs.google.com/spreadsheets/d/1cTLARjq-B5g50dL6tcntg7lb_Iu0ta43/"
    "edit?usp=sharing&ouid=109458533144345974874&rtpof=true&sd=true"
)

# ===================== ESTADO INICIAL =====================
def _ensure_state():
    """Garante chaves base na sessão (não re-inicialize em outros pontos)."""
    st.session_state.setdefault("catalogo_df", None)
    st.session_state.setdefault("kits_df", None)
    st.session_state.setdefault("loaded_at", None)
    st.session_state.setdefault("alt_sheet_link", DEFAULT_SHEET_LINK)
    st.session_state.setdefault("oc_cesta", pd.DataFrame())
    st.session_state.setdefault("compra_autom_data", {})

    for emp in ("ALIVVIA", "JCA"):
        st.session_state.setdefault(emp, {})
        st.session_state[emp].setdefault("FULL",    {"name": None, "bytes": None})
        st.session_state[emp].setdefault("VENDAS",  {"name": None, "bytes": None})
        st.session_state[emp].setdefault("ESTOQUE", {"name": None, "bytes": None})

_ensure_state()

# ===================== PERSISTÊNCIA EM DISCO (.uploads) =====================
BASE_DIR = Path(".uploads")
BASE_DIR.mkdir(exist_ok=True)

def _slug(s: str) -> str:
    return "".join(c if c.isalnum() or c in ("-", "_") else "-" for c in (s or "").upper())

def _empresa_dir(empresa: str) -> Path:
    p = BASE_DIR / _slug(empresa)
    p.mkdir(parents=True, exist_ok=True)
    return p

def _tipo_dir(empresa: str, tipo: str) -> Path:
    p = _empresa_dir(empresa) / _slug(tipo)
    p.mkdir(parents=True, exist_ok=True)
    return p

def _manifest_path(empresa: str) -> Path:
    return _empresa_dir(empresa) / "_manifest.json"

def _load_manifest(empresa: str) -> dict:
    mp = _manifest_path(empresa)
    if mp.exists():
        try:
            return json.loads(mp.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}

def _save_manifest(empresa: str, manifest: dict) -> None:
    _manifest_path(empresa).write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

def persist_to_disk(empresa: str, tipo: str, name: str, mime: str, data: bytes) -> Path:
    """Grava arquivo em .uploads/{EMPRESA}/{TIPO}/TIPO.ext e atualiza manifest."""
    ext = Path(name).suffix or ""
    fname = f"{_slug(tipo)}{ext}"
    fpath = _tipo_dir(empresa, tipo) / fname
    fpath.write_bytes(data)

    manifest = _load_manifest(empresa)
    manifest[tipo] = {
        "name": name,
        "mime": mime or "application/octet-stream",
        "path": str(fpath),
        "size": len(data),
        "sha1": hashlib.sha1(data).hexdigest(),
    }
    _save_manifest(empresa, manifest)
    return fpath

def remove_from_disk(empresa: str, tipo: str) -> None:
    manifest = _load_manifest(empresa)
    info = manifest.get(tipo)
    if info:
        try:
            Path(info["path"]).unlink(missing_ok=True)
        except Exception:
            pass
        manifest.pop(tipo, None)
        _save_manifest(empresa, manifest)

def load_from_disk_if_any(empresa: str, tipo: str) -> Optional[dict]:
    """Se houver arquivo no disco, devolve dict {name,mime,bytes}."""
    manifest = _load_manifest(empresa)
    info = manifest.get(tipo)
    if not info:
        return None
    p = Path(info["path"])
    if not p.exists():
        return None
    try:
        data = p.read_bytes()
        return {"name": info.get("name", p.name), "mime": info.get("mime", "application/octet-stream"), "bytes": data}
    except Exception:
        return None

def preload_persisted_uploads():
    """Se sessão estiver vazia, carrega do disco para sessão (ao iniciar o app)."""
    for emp in ("ALIVVIA", "JCA"):
        for tipo in ("FULL", "VENDAS", "ESTOQUE"):
            if not st.session_state[emp][tipo]["name"]:
                disk_item = load_from_disk_if_any(emp, tipo)
                if disk_item:
                    st.session_state[emp][tipo]["name"]  = disk_item["name"]
                    st.session_state[emp][tipo]["bytes"] = disk_item["bytes"]

preload_persisted_uploads()

# ===================== LOADERS PARA USO NAS ABAS =====================
def set_upload(empresa: str, tipo: str, uploaded_file) -> None:
    """Grava em sessão (nível 1)."""
    if uploaded_file is None:
        return
    st.session_state[empresa][tipo]["name"] = uploaded_file.name
    st.session_state[empresa][tipo]["bytes"] = uploaded_file.getbuffer().tobytes()

def clear_upload(empresa: str, tipo: str, also_disk: bool = True) -> None:
    st.session_state[empresa][tipo] = {"name": None, "bytes": None}
    if also_disk:
        remove_from_disk(empresa, tipo)

def df_from_saved(empresa: str, tipo: str) -> Optional[pd.DataFrame]:
    """Retorna DataFrame a partir do que está em sessão/disco."""
    item_name = st.session_state[empresa][tipo]["name"]
    item_bytes = st.session_state[empresa][tipo]["bytes"]
    if not item_name or not item_bytes:
        # fallback: tenta disco
        disk_item = load_from_disk_if_any(empresa, tipo)
        if not disk_item:
            return None
        item_name = disk_item["name"]
        item_bytes = disk_item["bytes"]
        st.session_state[empresa][tipo]["name"] = item_name
        st.session_state[empresa][tipo]["bytes"] = item_bytes

    name = (item_name or "").lower()
    bio = BytesIO(item_bytes)
    try:
        if name.endswith(".csv"):
            try:
                return pd.read_csv(bio)
            except Exception:
                bio.seek(0)
                return pd.read_csv(bio, sep=";")
        elif name.endswith(".xlsx") or name.endswith(".xls"):
            return pd.read_excel(bio, engine="openpyxl")
        else:
            st.error(f"{empresa}/{tipo}: formato não suportado ({item_name}).")
            return None
    except Exception as e:
        st.error(f"{empresa}/{tipo}: falha ao ler arquivo — {e}")
        return None

# ===================== SIDEBAR / PARÂMETROS =====================
with st.sidebar:
    st.subheader("Parâmetros")
    st.session_state.h  = st.selectbox("Horizonte (dias)", [30, 60, 90], index=1, key="h")
    st.session_state.g  = st.number_input("Crescimento % ao mês", value=0.0, step=1.0, key="g")
    st.session_state.LT = st.number_input("Lead time (dias)", value=0, step=1, min_value=0, key="LT")

    st.markdown("---")
    st.subheader("Padrão (KITS/CAT) — Google Sheets")
    st.caption("Carrega **somente** quando você clicar.")

    @st.cache_data(show_spinner="Baixando Planilha de Padrões KITS/CAT...")
    def get_padrao_from_sheets(sheet_id):
        content = logica_compra.baixar_xlsx_do_sheets(sheet_id)
        return logica_compra._carregar_padrao_de_content(content)

    colA, colB = st.columns([1, 1])
    with colA:
        if st.button("Carregar padrão agora", use_container_width=True):
            try:
                cat = get_padrao_from_sheets(DEFAULT_SHEET_ID)
                st.session_state.catalogo_df = cat.catalogo_simples.rename(columns={"component_sku": "sku"})
                st.session_state.kits_df = cat.kits_reais
                st.session_state.loaded_at = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                st.success("Padrão carregado com sucesso.")
            except Exception as e:
                st.session_state.catalogo_df = None
                st.session_state.kits_df = None
                st.session_state.loaded_at = None
                st.error(str(e))
    with colB:
        st.link_button("🔗 Abrir no Drive (editar)", DEFAULT_SHEET_LINK, use_container_width=True)

    st.text_input(
        "Link alternativo do Google Sheets (opcional)",
        key="alt_sheet_link",
        help="Se necessário, cole o link e use o botão abaixo.",
        value=st.session_state.get("alt_sheet_link") or DEFAULT_SHEET_LINK,
    )
    if st.button("Carregar deste link", use_container_width=True):
        try:
            content = logica_compra.baixar_xlsx_por_link_google(st.session_state.alt_sheet_link.strip())
            cat = logica_compra._carregar_padrao_de_content(content)
            st.session_state.catalogo_df = cat.catalogo_simples.rename(columns={"component_sku": "sku"})
            st.session_state.kits_df = cat.kits_reais
            st.session_state.loaded_at = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            st.success("Padrão carregado (link alternativo).")
        except Exception as e:
            st.session_state.catalogo_df = None
            st.session_state.kits_df = None
            st.session_state.loaded_at = None
            st.error(str(e))

# ===================== TÍTULO E ABAS =====================
st.title("Reposição Logística — Alivvia")
if st.session_state.catalogo_df is None or st.session_state.kits_df is None:
    st.warning("► Carregue o **Padrão (KITS/CAT)** no sidebar antes de usar as abas.")

tab1, tab2, tab3, tab4, tab5 = st.tabs(
    ["📂 Dados das Empresas", "🧮 Compra Automática", "📦 Alocação de Compra", "🛒 Ordem de Compra (OC)", "✨ Gerenciador de OCs"]
)

# ===================== TAB 1 — UPLOADS COM PERSISTÊNCIA =====================
with tab1:
    st.subheader("Uploads fixos por empresa (permanecem após F5)")
    st.caption("O arquivo é salvo na **sessão** ao enviar; com **Salvar (Confirmar)**, grava também em **.uploads/**.")

    def render_upload_slot(emp: str, slot: str, label: str, col):
        with col:
            st.markdown(f"**{label} — {emp}**")
            current_name = st.session_state[emp][slot]["name"]

            up_file = st.file_uploader("CSV/XLSX/XLS", type=["csv", "xlsx", "xls"], key=f"up_{slot}_{emp}")
            if up_file is not None:
                # Salva em sessão imediatamente (nível 1)
                set_upload(emp, slot, up_file)
                # Rerun para atualizar o box de status com o nome salvo
                st.rerun()

            # Status
            if current_name:
                st.info(f"💾 Salvo: {current_name}")

            c1, c2 = st.columns(2)
            with c1:
                if st.button(f"Salvar {label}", key=f"btn_save_{slot}_{emp}", use_container_width=True):
                    item_name = st.session_state[emp][slot]["name"]
                    item_bytes = st.session_state[emp][slot]["bytes"]
                    if not item_name or not item_bytes:
                        st.warning("Nada para salvar.")
                    else:
                        persist_to_disk(emp, slot, item_name, "application/octet-stream", item_bytes)
                        st.success("Confirmado e gravado em .uploads/")
            with c2:
                if st.button(f"Limpar {label}", key=f"btn_clear_{slot}_{emp}", use_container_width=True):
                    clear_upload(emp, slot, also_disk=True)
                    st.info("Limpo da sessão e do disco.")
                    st.rerun()

    def render_block(emp: str):
        st.markdown(f"### {emp}")
        c1, c2 = st.columns(2)
        render_upload_slot(emp, "FULL", "FULL", c1)
        render_upload_slot(emp, "VENDAS", "Shopee/MT (Vendas)", c2)

        st.markdown("---")
        c3, _ = st.columns([1, 1])
        render_upload_slot(emp, "ESTOQUE", "Estoque Físico", c3)

        st.markdown("---")
        # Ações em lote (empresa)
        b1, b2 = st.columns(2)
        with b1:
            if st.button(f"Salvar {emp} (Confirmar tudo)", key=f"save_all_{emp}", type="primary", use_container_width=True):
                faltando = []
                for slot in ("FULL", "VENDAS", "ESTOQUE"):
                    nm = st.session_state[emp][slot]["name"]
                    bt = st.session_state[emp][slot]["bytes"]
                    if not nm or not bt:
                        faltando.append(slot)
                        continue
                    persist_to_disk(emp, slot, nm, "application/octet-stream", bt)
                if faltando:
                    st.warning(f"{emp}: faltou salvar {', '.join(faltando)}.")
                else:
                    st.success(f"{emp}: todos os arquivos confirmados e gravados em .uploads/")
        with b2:
            if st.button(f"Limpar {emp} (Tudo)", key=f"clear_all_{emp}", use_container_width=True):
                for slot in ("FULL", "VENDAS", "ESTOQUE"):
                    clear_upload(emp, slot, also_disk=True)
                st.info(f"{emp}: tudo limpo de sessão e disco.")
                st.rerun()

    # Renderiza ALIVVIA e JCA
    render_block("ALIVVIA")
    render_block("JCA")

    st.markdown("## ⚠️ Limpeza Total")
    if st.button("🔴 Limpar TUDO (ALIVVIA e JCA)", key="clr_all_global", type="primary", use_container_width=True):
        for emp in ("ALIVVIA", "JCA"):
            for slot in ("FULL", "VENDAS", "ESTOQUE"):
                clear_upload(emp, slot, also_disk=True)
        st.info("Todos os dados foram limpos (sessão + disco).")
        st.rerun()

    with st.expander("Prévia (opcional)"):
        for emp in ("ALIVVIA", "JCA"):
            st.caption(f"Arquivos de {emp}")
            c1, c2, c3 = st.columns(3)
            for col, slot in zip((c1, c2, c3), ("FULL", "VENDAS", "ESTOQUE")):
                with col:
                    dfp = df_from_saved(emp, slot)
                    if dfp is not None:
                        st.caption(f"{slot}: {dfp.shape[0]} linhas / {dfp.shape[1]} colunas")
                        st.dataframe(dfp.head(5), use_container_width=True, hide_index=True)
                    else:
                        st.caption(f"{slot}: (vazio)")

# ===================== TAB 2 — COMPRA AUTOMÁTICA =====================
with tab2:
    # As funções internas dos módulos devem usar os bytes armazenados na sessão:
    # st.session_state["ALIVVIA"]["FULL"]["bytes"], etc., ou chamar df_from_saved(...)
    mod_compra_autom.render_tab2(st.session_state, st.session_state.h, st.session_state.g, st.session_state.LT)

# ===================== TAB 3 — ALOCAÇÃO DE COMPRA =====================
with tab3:
    mod_alocacao.render_tab3(st.session_state)

# ===================== TAB 4 — ORDEM DE COMPRA (placeholder) =====================
with tab4:
    if ordem_compra:
        try:
            ordem_compra.render_tab4(st.session_state)
        except Exception as e:
            st.error(f"Erro na Tab 4: {e}")
    else:
        st.info("Módulo 'ordem_compra' indisponível neste ambiente.")

# ===================== TAB 5 — GERENCIADOR DE OCs (placeholder) =====================
with tab5:
    if gerenciador_oc:
        try:
            gerenciador_oc.render_tab5(st.session_state)
        except Exception as e:
            st.error(f"Erro na Tab 5: {e}")
    else:
        st.info("Módulo 'gerenciador_oc' indisponível neste ambiente.")

st.caption(f"© Alivvia — simples, robusto e auditável. ({VERSION})")
