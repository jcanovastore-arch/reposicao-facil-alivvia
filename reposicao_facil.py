# reposicao_facil.py - ESTABILIDADE V10.9 (Fix Cesta + Reativa Tab 3)
# - Corrige o bug da cesta (oc_cesta -> oc_cesta_itens)
# - Reativa o nome da Tab 3 ("Alocação de Compra")

import datetime as dt
import json
import io
import hashlib
from pathlib import Path
from typing import Optional

import pandas as pd
import streamlit as st

# ====== MÓDulos DO PROJETO ======
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

# MÓDULOS DE OC (opcionais)
try:
    import ordem_compra
except Exception:
    ordem_compra = None

try:
    import gerenciador_oc
except Exception:
    gerenciador_oc = None

VERSION = "v10.9 – Crash Fix + Fluxo Alocação Correto"

# ===================== CONFIG PÁGINA =====================
st.set_page_config(page_title="Reposição Logística — Alivvia", layout="wide")

DEFAULT_SHEET_LINK = (
    "https://docs.google.com/spreadsheets/d/1cTLARjq-B5g50dL6tcntg7lb_Iu0ta43/"
    "edit?usp=sharing&ouid=109458533144345974874&rtpof=true&sd=true"
)

# ===================== ESTADO INICIAL (CORRIGIDO V10.9) =====================
def _ensure_state():
    st.session_state.setdefault("catalogo_df", None)
    st.session_state.setdefault("kits_df", None)
    st.session_state.setdefault("loaded_at", None)
    st.session_state.setdefault("alt_sheet_link", DEFAULT_SHEET_LINK)
    
    # CHAVE CORRETA (Fix V10.7)
    st.session_state.setdefault("oc_cesta_itens", {"ALIVVIA": [], "JCA": []})
    
    st.session_state.setdefault("compra_autom_data", {})
    for emp in ("ALIVVIA", "JCA"):
        st.session_state.setdefault(emp, {})
        st.session_state[emp].setdefault("FULL",    {"name": None, "bytes": None})
        st.session_state[emp].setdefault("VENDAS",  {"name": None, "bytes": None})
        st.session_state[emp].setdefault("ESTOQUE", {"name": None, "bytes": None})

_ensure_state()

# ===================== PERSISTÊNCIA LOCAL (.uploads) =====================
BASE_DIR = Path(".uploads")
BASE_DIR.mkdir(exist_ok=True)

def _slug(s: str) -> str:
    s = (s or "").strip()
    return "".join(c if c.isalnum() or c in ("-", "_") else "-" for c in s.upper())

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
    _manifest_path(empresa).write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )

def persist_to_disk(empresa: str, tipo: str, name: str, mime: str, data: bytes) -> Path:
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
        "saved_at": dt.datetime.now().isoformat(timespec="seconds"),
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
    manifest = _load_manifest(empresa)
    info = manifest.get(tipo)
    if not info:
        return None
    p = Path(info["path"])
    if not p.exists():
        return None
    try:
        data = p.read_bytes()
        return {
            "name": info.get("name", p.name),
            "mime": info.get("mime", "application/octet-stream"),
            "bytes": data,
            "sha1": info.get("sha1"),
            "saved_at": info.get("saved_at"),
        }
    except Exception:
        return None

def preload_persisted_uploads():
    """Se sessão estiver vazia, carrega do disco para sessão (ao iniciar/entrar na Tab 1)."""
    for emp in ("ALIVVIA", "JCA"):
        for tipo in ("FULL", "VENDAS", "ESTOQUE"):
            if not st.session_state[emp][tipo]["name"]:
                disk_item = load_from_disk_if_any(emp, tipo)
                if disk_item:
                    st.session_state[emp][tipo]["name"] = disk_item["name"]
                    st.session_state[emp][tipo]["bytes"] = disk_item["bytes"]

preload_persisted_uploads()

# ===================== HELPERS DE DATAFRAME / PARSING CACHEADO =====================
@st.cache_data(show_spinner=False)
def _parse_table_cached(name_lower: str, raw_bytes: bytes) -> Optional[pd.DataFrame]:
    if not name_lower or not raw_bytes:
        return None
    _ = hashlib.sha1(raw_bytes).hexdigest()
    bio = io.BytesIO(raw_bytes)
    try:
        if name_lower.endswith(".csv"):
            try:
                return pd.read_csv(bio)
            except Exception:
                bio.seek(0)
                return pd.read_csv(bio, sep=";")
        elif name_lower.endswith(".xlsx") or name_lower.endswith(".xls"):
            return pd.read_excel(bio, engine="openpyxl")
        else:
            return None
    except Exception:
        return None

def df_from_saved_cached(empresa: str, tipo: str) -> Optional[pd.DataFrame]:
    item_name = st.session_state[empresa][tipo]["name"]
    item_bytes = st.session_state[empresa][tipo]["bytes"]

    if not item_name or not item_bytes:
        disk_item = load_from_disk_if_any(empresa, tipo)
        if not disk_item:
            return None
        item_name = disk_item["name"]
        item_bytes = disk_item["bytes"]
        st.session_state[empresa][tipo]["name"] = item_name
        st.session_state[empresa][tipo]["bytes"] = item_bytes

    return _parse_table_cached((item_name or "").lower(), item_bytes)

def clear_upload(empresa: str, tipo: str, also_disk: bool = True) -> None:
    st.session_state[empresa][tipo] = {"name": None, "bytes": None}
    if also_disk:
        remove_from_disk(empresa, tipo)

# ===================== SIDEBAR / PARÂMETROS =====================
with st.sidebar:
    st.subheader("Parâmetros")
    h  = st.selectbox("Horizonte (dias)", [30, 60, 90], index=1, key="h")
    g  = st.number_input("Crescimento % ao mês", value=0.0, step=1.0, key="g")
    LT = st.number_input("Lead time (dias)", value=0, step=1, min_value=0, key="LT")

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
                st.session_state.catalogo_df = cat.catalogo_simples.rename(columns={"component_sku":"sku"})
                st.session_state.kits_df = cat.kits_reais
                st.session_state.loaded_at = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                st.success("Padrão carregado com sucesso.")
            except Exception as e:
                st.session_state.catalogo_df = None
                st.session_state.kits_df = None
                st.session_state.loaded_at = None
                st.error(f"Erro ao carregar padrão: {str(e)}")
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
            st.session_state.catalogo_df = cat.catalogo_simples.rename(columns={"component_sku":"sku"})
            st.session_state.kits_df = cat.kits_reais
            st.session_state.loaded_at = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            st.success("Padrão carregado (link alternativo).")
        except Exception as e:
            st.session_state.catalogo_df = None
            st.session_state.kits_df = None
            st.session_state.loaded_at = None
            st.error(f"Erro ao carregar do link: {str(e)}")

# ===================== TÍTULO E ABAS (CORRIGIDO V10.9) =====================
st.title("Reposição Logística — Alivvia")
if st.session_state.catalogo_df is None or st.session_state.kits_df is None:
    st.warning("► Carregue o **Padrão (KITS/CAT)** no sidebar antes de usar as abas.")

# =================================================================
# >> INÍCIO DA CORREÇÃO (V10.9) - Reativa Tab 3 <<
# =================================================================
tab1, tab2, tab3, tab4, tab5 = st.tabs(
    ["📂 Dados das Empresas", "🧮 Compra Automática", "📦 Alocação de Compra", "🛒 Ordem de Compra (OC)", "✨ Gerenciador de OCs"]
)
# =================================================================
# >> FIM DA CORREÇÃO (V10.9) <<
# =================================================================

# ===================== TAB 1 — UPLOADS COM PERSISTÊNCIA (ANTI-FLICKER) =====================
with tab1:
    st.subheader("Uploads fixos por empresa (sessão + disco)")
    st.caption("Após **Salvar (Confirmar)**, o arquivo fica gravado em .uploads/ e volta sozinho após F5/restart.")

    def render_upload_slot(emp: str, slot: str, label: str, col):
        with col:
            st.markdown(f"**{label} — {emp}**")
            up_file = st.file_uploader(
                "CSV/XLSX/XLS",
                type=["csv", "xlsx", "xls"],
                key=f"up_{slot}_{emp}"
            )
            if up_file is not None:
                st.session_state[emp][slot]["name"] = up_file.name
                st.session_state[emp][slot]["bytes"] = up_file.getbuffer().tobytes()
                st.info(f"💾 Salvo na sessão: {up_file.name}")
            c1, c2 = st.columns(2)
            with c1:
                if st.button(f"Salvar {label} (Confirmar)", key=f"btn_save_{slot}_{emp}", use_container_width=True):
                    nm = st.session_state[emp][slot]["name"]
                    bt = st.session_state[emp][slot]["bytes"]
                    if not nm or not bt:
                        st.warning("Nada para salvar.")
                    else:
                        persist_to_disk(emp, slot, nm, "application/octet-stream", bt)
                        st.success("✅ Confirmado em .uploads/")
            with c2:
                if st.button(f"Limpar {label}", key=f"btn_clear_{slot}_{emp}", use_container_width=True):
                    clear_upload(emp, slot, also_disk=True)
                    st.info("Removido da sessão e do disco.")
            disk_info = load_from_disk_if_any(emp, slot)
            if disk_info:
                short_sha = (disk_info.get("sha1") or "")[:8]
                when = disk_info.get("saved_at") or "-"
                st.caption(f"📦 Disco: {disk_info['name']} • {short_sha} • {when}")
            with st.expander("Prévia (opcional)"):
                dfp = df_from_saved_cached(emp, slot)
                if dfp is not None:
                    st.caption(f"{label}: {dfp.shape[0]} linhas / {dfp.shape[1]} colunas")
                    st.dataframe(dfp.head(5), use_container_width=True, hide_index=True)
                else:
                    st.caption("(vazio)")

    def render_block(emp: str):
        st.markdown(f"### {emp}")
        c1, c2 = st.columns(2)
        render_upload_slot(emp, "FULL", "FULL", c1)
        render_upload_slot(emp, "VENDAS", "Shopee/MT (Vendas)", c2)
        st.markdown("---")
        c3, _ = st.columns([1, 1])
        render_upload_slot(emp, "ESTOQUE", "Estoque Físico", c3)
        st.markdown("---")
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
                    st.success(f"{emp}: todos os arquivos confirmados em .uploads/")
        with b2:
            if st.button(f"Limpar {emp} (Tudo)", key=f"clear_all_{emp}", use_container_width=True):
                for slot in ("FULL", "VENDAS", "ESTOQUE"):
                    clear_upload(emp, slot, also_disk=True)
                st.info(f"{emp}: sessão e disco limpos.")
    
    render_block("ALIVVIA")
    render_block("JCA")

    st.markdown("## ⚠️ Limpeza Total")
    if st.button("🔴 Limpar TUDO (ALIVVIA e JCA)", key="clr_all_global", type="primary", use_container_width=True):
        for emp in ("ALIVVIA", "JCA"):
            for slot in ("FULL", "VENDAS", "ESTOQUE"):
                clear_upload(emp, slot, also_disk=True)
        st.info("Todos os dados foram limpos (sessão + disco).")

# ===================== TAB 2 — COMPRA AUTOMÁTICA =====================
with tab2:
    h_val = h
    g_val = g
    lt_val = LT
    mod_compra_autom.render_tab2(st.session_state, h_val, g_val, lt_val)

# ===================== TAB 3 — ALOCAÇÃO DE COMPRA =====================
with tab3:
    mod_alocacao.render_tab3(st.session_state)

# ===================== TAB 4 / TAB 5 (se existirem) =====================
with tab4:
    if ordem_compra:
        try:
            ordem_compra.render_tab4(st.session_state)
        except Exception as e:
            st.error(f"Erro na Tab 4: {e}")
    else:
        st.info("Módulo 'ordem_compra' indisponível neste ambiente.")

with tab5:
    if gerenciador_oc:
        try:
            gerenciador_oc.render_tab5(st.session_state)
        except Exception as e:
            st.error(f"Erro na Tab 5: {e}")
    else:
        st.info("Módulo 'gerenciador_oc' indisponível neste ambiente.")

st.caption(f"© Alivvia — simples, robusto e auditável. ({VERSION})")