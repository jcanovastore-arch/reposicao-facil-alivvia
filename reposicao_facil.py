# reposicao_facil.py
# Reposição Logística — Alivvia (Streamlit)
# ARQUITETURA CONSOLIDADA V3.2.4 (FIX FINAL DEFINITIVO: SELEÇÃO POR LISTA DE SKUS + INDENTAÇÃO CORRIGIDA)

import io
import re
import hashlib
import datetime as dt
from dataclasses import dataclass
from typing import Optional, Tuple
import os 
import time

import numpy as np
import pandas as pd
from unidecode import unidecode
import streamlit as st
import requests
from requests.adapters import HTTPAdapter, Retry

# ===================== CONFIG BÁSICA =====================
st.set_page_config(page_title="Reposição Logística — Alivvia", layout="wide")

DEFAULT_SHEET_LINK = (
    "https://docs.google.com/spreadsheets/d/1cTLARjq-B5g50dL6tcntg7lb_Iu0ta43/"
    "edit?usp=sharing&ouid=109458533144345974874&rtpof=true&sd=true"
)
DEFAULT_SHEET_ID = "1cTLARjq-B5g50dL6tcntg7lb_Iu0ta43"  # fixo

# NOVO V3.2: Arquivo local para carregamento prioritário
LOCAL_PADRAO_FILENAME = "Padrao_produtos.xlsx" 

# Diretório de persistência de uploads no disco (herdado do V2.5)
STORAGE_DIR = ".streamlit/uploaded_files_cache"
if not os.path.exists(STORAGE_DIR):
    os.makedirs(STORAGE_DIR, exist_ok=True)

# Helper function para caminho determinístico no disco
def get_local_file_path(empresa: str, tipo: str) -> str:
    return os.path.join(STORAGE_DIR, f"{empresa}_{tipo}.bin")

# Helper function para caminho determinístico do nome original
def get_local_name_path(empresa: str, tipo: str) -> str:
    return os.path.join(STORAGE_DIR, f"{empresa}_{tipo}_name.txt")


# ===================== ESTADO =====================
def _ensure_state():
    st.session_state.setdefault("catalogo_df", None)
    st.session_state.setdefault("kits_df", None)
    st.session_state.setdefault("loaded_at", None)
    st.session_state.setdefault("alt_sheet_link", DEFAULT_SHEET_LINK)

    st.session_state.setdefault("resultado_ALIVVIA", None)
    st.session_state.setdefault("resultado_JCA", None)
    st.session_state.setdefault("carrinho_compras", [])

    # FIX V3.2.4: SKUs selecionados para compra via input de texto (para evitar bugs de estado)
    st.session_state.setdefault('selected_skus_input', "")

    # uploads por empresa
    for emp in ["ALIVVIA", "JCA"]:
        st.session_state.setdefault(emp, {})
        for file_type in ["FULL", "VENDAS", "ESTOQUE"]:
            state = st.session_state[emp].setdefault(file_type, {"name": None, "bytes": None})
            
            # Tenta carregar do disco na inicialização
            if not state["name"]:
                path_bin = get_local_file_path(emp, file_type)
                path_name = get_local_name_path(emp, file_type)
                
                if os.path.exists(path_bin) and os.path.exists(path_name):
                    try:
                        with open(path_bin, 'rb') as f_bin:
                            state["bytes"] = f_bin.read()
                        with open(path_name, 'r', encoding='utf-8') as f_name:
                            state["name"] = f_name.read().strip()
                        state['is_cached'] = True
                    except Exception:
                        state["name"] = None; state["bytes"] = None


_ensure_state()

# ===================== HTTP / GOOGLE SHEETS =====================
def _requests_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(total=3, backoff_factor=0.6, status_forcelist=[429,500,502,503,504], allowed_methods=["GET"])
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.headers.update({"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125 Safari/537.36"})
    return s

def gs_export_xlsx_url(sheet_id: str) -> str:
    return f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=xlsx"

def extract_sheet_id_from_url(url: str) -> Optional[str]:
    if not url: return None
    m = re.search(r"/d/([a-zA-Z0-9\-_]+)/", url)
    return m.group(1) if m else None

def baixar_xlsx_por_link_google(url: str) -> bytes:
    s = _requests_session()
    if "export?format=xlsx" in url:
        r = s.get(url, timeout=30); r.raise_for_status(); return r.content
    sid = extract_sheet_id_from_url(url)
    if not sid: raise RuntimeError("Link inválido do Google Sheets (esperado .../d/<ID>/...).")
    r = s.get(gs_export_xlsx_url(sid), timeout=30); r.raise_for_status(); return r.content

def baixar_xlsx_do_sheets(sheet_id: str) -> bytes:
    s = _requests_session()
    url = gs_export_xlsx_url(sheet_id)
    try:
        r = s.get(url, timeout=30)
        r.raise_for_status()
    except requests.HTTPError as e:
        sc = getattr(e.response, "status_code", "?")
        raise RuntimeError(
            f"Falha ao baixar XLSX (HTTP {sc}). Verifique: compartilhamento 'Qualquer pessoa com link – Leitor'.\nURL: {url}"
        )
    return r.content

# ===================== UTILS DE DADOS =====================
def norm_header(s: str) -> str:
    s = (s or "").strip()
    s = unidecode(s).lower()
    for ch in [" ", "-", "(", ")", "/", "\\", "[", "]", ".", ",", ";", ":"]:
        s = s.replace(ch, "_")
    while "__" in s:
        s = s.replace("__", "_")
    return s.strip("_")

def normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [norm_header(c) for c in df.columns]
    return df

def br_to_float(x):
    if pd.isna(x): return np.nan
    if isinstance(x,(int,float,np.integer,np.floating)): return float(x)
    s = str(x).strip()
    if s == "": return np.nan
    s = s.replace("\u00a0"," ").replace("R$","").replace(" ","").replace(".","").replace(",",".")
    try: return float(s)
    except: return np.nan

def norm_sku(x: str) -> str:
    if pd.isna(x): return ""
    return unidecode(str(x)).strip().upper()

def exige_colunas(df: pd.DataFrame, obrig: list, nome: str):
    faltam = [c for c in obrig if c not in df.columns]
    if faltam:
        raise ValueError(f"Colunas obrigatórias ausentes em {nome}: {faltam}\nColunas lidas: {list(df.columns)}")

# Função para forçar os tipos numéricos antes de estilizar (CORREÇÃO DE ERRO)
def enforce_numeric_types(df: pd.DataFrame) -> pd.DataFrame:
    """Garante que colunas numéricas chave sejam float ou int para o Styler."""
    df = df.copy()
    
    # Colunas que devem ser tratadas como float (moeda/custo)
    for col in ["Preco", "Valor_Compra_R$", "Preco_Custo", "Valor_Sugerido_R$", "Valor_Ajustado_R$"]:
        if col in df.columns:
            # Converte para float, convertendo erros (strings, etc.) para NaN, ARREDONDA para 2 casas e converte.
            df[col] = pd.to_numeric(df[col], errors='coerce').round(2).astype(float)
            
    # Colunas que devem ser tratadas como inteiros (quantidade)
    for col in ["Vendas_Total_60d", "Estoque_Full", "Estoque_Fisico", "Compra_Sugerida", "Qtd_Sugerida", "Qtd_Ajustada", "Em_Transito"]:
        if col in df.columns:
            # Converte para numérico (erros para NaN), preenche NaN com 0 e converte para int
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
            
    return df

# ===================== LEITURA DE ARQUIVOS =====================
def load_any_table(uploaded_file) -> Optional[pd.DataFrame]:
    if uploaded_file is None:
        return None
    name = uploaded_file.name.lower()
    try:
        if name.endswith(".csv"):
            uploaded_file.seek(0)
            df = pd.read_csv(uploaded_file, dtype=str, keep_default_na=False, sep=None, engine="python")
        else:
            df = pd.read_excel(uploaded_file, dtype=str, keep_default_na=False)
    except Exception as e:
        raise RuntimeError(f"Não consegui ler o arquivo '{uploaded_file.name}': {e}")

    df.columns = [norm_header(c) for c in df.columns]

    # fallback header=2 (FULL Magiic)
    tem_col_sku = any(c in df.columns for c in ["sku","codigo","codigo_sku"]) or any("sku" in c for c in df.columns)
    if (not tem_col_sku) and (len(df) > 0):
        try:
            uploaded_file.seek(0)
            if name.endswith(".csv"):
                df = pd.read_csv(uploaded_file, dtype=str, keep_default_na=False, header=2)
            else:
                df = pd.read_excel(uploaded_file, dtype=str, keep_default_na=False, header=2)
            df.columns = [norm_header(c) for c in df.columns]
        except Exception:
            pass

    # limpeza
    cols = set(df.columns)
    sku_col = next((c for c in ["sku","codigo","codigo_sku"] if c in cols), None)
    if sku_col:
        df[sku_col] = df[sku_col].map(norm_sku)
        df = df[df[sku_col] != ""]
    for c in list(df.columns):
        df = df[~df[c].astype(str).str.contains(r"^TOTALS?$|^TOTAIS?$", case=False, na=False)]
    return df.reset_index(drop=True)

def load_any_table_from_bytes(file_name: str, blob: bytes) -> pd.DataFrame:
    """Leitura a partir de bytes salvos na sessão (com fallback header=2)."""
    bio = io.BytesIO(blob); name = (file_name or "").lower()
    try:
        if name.endswith(".csv"):
            df = pd.read_csv(bio, dtype=str, keep_default_na=False, sep=None, engine="python")
        else:
            df = pd.read_excel(bio, dtype=str, keep_default_na=False)
    except Exception as e:
        raise RuntimeError(f"Não consegui ler o arquivo salvo '{file_name}': {e}")

    df.columns = [norm_header(c) for c in df.columns]
    tem_col_sku = any(c in df.columns for c in ["sku","codigo","codigo_sku"]) or any("sku" in c for c in df.columns)
    if (not tem_col_sku) and (len(df) > 0):
        try:
            bio.seek(0)
            if name.endswith(".csv"):
                df = pd.read_csv(bio, dtype=str, keep_default_na=False, header=2)
            else:
                df = pd.read_excel(bio, dtype=str, keep_default_na=False, header=2)
            df.columns = [norm_header(c) for c in df.columns]
        except Exception:
            pass

    cols = set(df.columns)
    sku_col = next((c for c in ["sku","codigo","codigo_sku"] if c in cols), None)
    if sku_col:
        df[sku_col] = df[sku_col].map(norm_sku)
        df = df[df[sku_col] != ""]
    for c in list(df.columns):
        df = df[~df[c].astype(str).str.contains(r"^TOTALS?$|^TOTAIS?$", case=False, na=False)]
    return df.reset_index(drop=True)

# ===================== PADRÃO KITS/CAT =====================
@dataclass
class Catalogo:
    catalogo_simples: pd.DataFrame  # component_sku, fornecedor, status_reposicao
    kits_reais: pd.DataFrame        # kit_sku, component_sku, qty

def _carregar_padrao_de_content(content: bytes) -> Catalogo:
    try:
        xls = pd.ExcelFile(io.BytesIO(content))
    except Exception as e:
        raise RuntimeError(f"Arquivo XLSX inválido: {e}")

    def load_sheet(opts):
        for n in opts:
            if n in xls.sheet_names:
                return pd.read_excel(xls, n, dtype=str, keep_default_na=False)
        raise RuntimeError(f"Aba não encontrada. Esperado uma de {opts}. Abas: {xls.sheet_names}")

    df_kits = load_sheet(["KITS","KITS_REAIS","kits","kits_reais"]).copy()
    df_cat  = load_sheet(["CATALOGO_SIMPLES","CATALOGO","catalogo_simples","catalogo"]).copy()

    # KITS
    df_kits = normalize_cols(df_kits)
    possiveis_kits = {
        "kit_sku": ["kit_sku", "kit", "sku_kit"],
        "component_sku": ["component_sku","componente","sku_componente","component","sku_component"],
        "qty": ["qty","qty_por_kit","qtd_por_kit","quantidade_por_kit","qtd","quantidade"]
    }
    rename_k = {}
    for alvo, cand in possiveis_kits.items():
        for c in cand:
            if c in df_kits.columns:
                rename_k[c] = alvo; break
    df_kits = df_kits.rename(columns=rename_k)
    exige_colunas(df_kits, ["kit_sku","component_sku","qty"], "KITS")
    df_kits = df_kits[["kit_sku","component_sku","qty"]].copy()
    df_kits["kit_sku"] = df_kits["kit_sku"].map(norm_sku)
    df_kits["component_sku"] = df_kits["component_sku"].map(norm_sku)
    df_kits["qty"] = df_kits["qty"].map(br_to_float).fillna(0).astype(int)
    # Garante que não há kits/componentes duplicados
    df_kits = df_kits[df_kits["qty"] >= 1].drop_duplicates(subset=["kit_sku","component_sku"], keep="first")

    # CATALOGO
    df_cat = normalize_cols(df_cat)
    possiveis_cat = {
        "component_sku": ["component_sku","sku","produto","item","codigo","sku_componente"],
        "fornecedor": ["fornecedor","supplier","fab","marca"],
        "status_reposicao": ["status_reposicao","status","reposicao_status"]
    }
    rename_c = {}
    for alvo, cand in possiveis_cat.items():
        for c in cand:
            if c in df_cat.columns:
                rename_c[c] = alvo; break
    df_cat = df_cat.rename(columns=rename_c)
    if "component_sku" not in df_cat.columns:
        raise ValueError("CATALOGO precisa ter a coluna 'component_sku' (ou 'sku').")
    if "fornecedor" not in df_cat.columns:
        df_cat["fornecedor"] = ""
    if "status_reposicao" not in df_cat.columns:
        df_cat["status_reposicao"] = ""
    df_cat["component_sku"] = df_cat["component_sku"].map(norm_sku)
    df_cat["fornecedor"] = df_cat["fornecedor"].fillna("").astype(str)
    df_cat["status_reposicao"] = df_cat["status_reposicao"].fillna("").astype(str)
    
    # GARANTE SKUS ÚNICOS e FILTRO DE NÃO REPOR NA ORIGEM (FIX V2.9)
    # Filtra SKUs que contêm "nao_repor" no status (case insensitive)
    mask_repor = ~df_cat["status_reposicao"].str.lower().str.contains("nao_repor", na=False)
    df_cat = df_cat[mask_repor].copy()
    
    # Remove duplicatas no catálogo (mantém o último)
    df_cat = df_cat.drop_duplicates(subset=["component_sku"], keep="last").reset_index(drop=True)

    return Catalogo(catalogo_simples=df_cat, kits_reais=df_kits)

def carregar_padrao_do_xlsx(sheet_id: str) -> Catalogo:
    content = baixar_xlsx_do_sheets(sheet_id)
    return _carregar_padrao_de_content(content)

def carregar_padrao_do_link(url: str) -> Catalogo:
    content = baixar_xlsx_por_link_google(url)
    return _carregar_padrao_de_content(content)

# NOVO V3.2: Função para tentar carregar localmente ou do Sheets
def carregar_padrao_local_ou_sheets(sheet_link: str) -> Tuple[Catalogo, str]:
    # 1. Tenta carregar do arquivo local
    if os.path.exists(LOCAL_PADRAO_FILENAME):
        try:
            with open(LOCAL_PADRAO_FILENAME, 'rb') as f:
                content = f.read()
            return _carregar_padrao_de_content(content), "local"
        except Exception as e:
            st.warning(f"Falha ao ler arquivo local '{LOCAL_PADRAO_FILENAME}'. Tentando baixar do Google Sheets. Erro: {e}")
            time.sleep(0.5)

    # 2. Se falhar ou não existir, baixa do Google Sheets
    try:
        sid = extract_sheet_id_from_url(sheet_link)
        if not sid:
            raise RuntimeError("Link inválido do Google Sheets.")
        
        content = baixar_xlsx_do_sheets(sid)
        return _carregar_padrao_de_content(content), "sheets"
    except Exception as e:
        raise RuntimeError(f"Falha ao carregar o Padrão do Google Sheets. Erro: {e}")

def construir_kits_efetivo(cat: Catalogo) -> pd.DataFrame:
    kits = cat.kits_reais.copy()
    
    # Obtém os SKUs únicos no catálogo de itens que DEVEM ser repostos
    componentes_validos = set(cat.catalogo_simples["component_sku"].unique())
    kits_validos = set(kits["kit_sku"].unique())
    
    # 1. Filtra kits e componentes no kits_reais para garantir que só contenha componentes válidos
    kits = kits[kits["component_sku"].isin(componentes_validos)].copy()
    
    # 2. Adiciona o alias (SKU simples) apenas se for um componente válido e NÃO for um kit
    alias = []
    for s in componentes_validos:
        s = norm_sku(s)
        # Adiciona como alias se o SKU existe no catalogo (válido) e não é um kit principal
        if s and s not in kits_validos:
            alias.append((s, s, 1))
            
    if alias:
        kits_df_alias = pd.DataFrame(alias, columns=["kit_sku","component_sku","qty"])
        kits = pd.concat([kits, kits_df_alias], ignore_index=True)
        
    # Garante unicidade e remove SKUs inválidos
    kits = kits.drop_duplicates(subset=["kit_sku","component_sku"], keep="first")
    return kits

# ===================== MAPEAMENTO FULL/FISICO/VENDAS =====================
def mapear_tipo(df: pd.DataFrame) -> str:
    cols = [c.lower() for c in df.columns]
    tem_sku_std  = any(c in {"sku","codigo","codigo_sku"} for c in cols) or any("sku" in c for c in cols)
    tem_vendas60 = any(c.startswith("vendas_60d") or c in {"vendas 60d","vendas_qtd_60d"} for c in cols)
    tem_qtd_livre= any(("qtde" in c) or ("quant" in c) or ("venda" in c) or ("order" in c) for c in cols)
    tem_estoque_full_like = any(("estoque" in c and "full" in c) or c=="estoque_full" for c in cols)
    tem_estoque_generico  = any(c in {"estoque_atual","qtd","quantidade"} or "estoque" in c for c in cols)
    tem_transito_like     = any(("transito" in c) or c in {"em_transito","em transito","em_transito_full","em_transito_do_anuncio"} for c in cols)
    tem_preco = any(c in {"preco","preco_compra","preco_medio","custo","custo_medio"} for c in cols)

    if tem_sku_std and (tem_vendas60 or tem_estoque_full_like or tem_transito_like):
        return "FULL"
    if tem_sku_std and tem_vendas60 and tem_qtd_livre:
        return "FULL" # Confirma FULL (mais robusto)
    if tem_sku_std and tem_estoque_generico and tem_preco:
        return "FISICO"
    if tem_sku_std and tem_qtd_livre and not tem_preco:
        return "VENDAS"
    return "DESCONHECIDO"

def mapear_colunas(df: pd.DataFrame, tipo: str) -> pd.DataFrame:
    if tipo == "FULL":
        if "sku" in df.columns:           df["SKU"] = df["sku"].map(norm_sku)
        elif "codigo" in df.columns:      df["SKU"] = df["codigo"].map(norm_sku)
        elif "codigo_sku" in df.columns:  df["SKU"] = df["codigo_sku"].map(norm_sku)
        else: raise RuntimeError("FULL inválido: precisa de coluna SKU/codigo.")

        c_v = [c for c in df.columns if c in ["vendas_qtd_60d","vendas_60d","vendas 60d"] or c.startswith("vendas_60d")]
        if not c_v: raise RuntimeError("FULL inválido: faltou Vendas_60d.")
        df["Vendas_Qtd_60d"] = df[c_v[0]].map(br_to_float).fillna(0).astype(int)

        c_e = [c for c in df.columns if c in ["estoque_full","estoque_atual"] or ("estoque" in c and "full" in c)]
        if not c_e: raise RuntimeError("FULL inválido: faltou Estoque_Full/estoque_atual.")
        df["Estoque_Full"] = df[c_e[0]].map(br_to_float).fillna(0).astype(int)

        c_t = [c for c in df.columns if c in ["em_transito","em transito","em_transito_full","em_transito_do_anuncio"] or ("transito" in c)]
        # FIX V3.0: Garante que a coluna Em_Transito exista, mesmo que seja 0.
        df["Em_Transito"] = df[c_t[0]].map(br_to_float).fillna(0).astype(int) if c_t else 0 

        return df[["SKU","Vendas_Qtd_60d","Estoque_Full","Em_Transito"]].copy()

    if tipo == "FISICO":
        sku_series = (
            df["sku"] if "sku" in df.columns else
            (df["codigo"] if "codigo" in df.columns else
             (df["codigo_sku"] if "codigo_sku" in df.columns else None))
        )
        if sku_series is None:
            cand = next((c for c in df.columns if "sku" in c.lower()), None)
            if cand is None: raise RuntimeError("FÍSICO inválido: não achei coluna de SKU.")
            sku_series = df[cand]
        df["SKU"] = sku_series.map(norm_sku)

        c_q = [c for c in df.columns if c in ["estoque_atual","qtd","quantidade"] or ("estoque" in c)]
        if not c_q: raise RuntimeError("FÍSICO inválido: faltou Estoque.")
        df["Estoque_Fisico"] = df[c_q[0]].map(br_to_float).fillna(0).astype(int)

        c_p = [c for c in df.columns if c in ["preco","preco_compra","custo","custo_medio","preco_medio","preco_unitario"]]
        if not c_p: raise RuntimeError("FÍSICO inválido: faltou Preço/Custo.")
        df["Preco"] = df[c_p[0]].map(br_to_float).fillna(0.0)

        return df[["SKU","Estoque_Fisico","Preco"]].copy()

    if tipo == "VENDAS":
        sku_col = next((c for c in df.columns if "sku" in c.lower()), None)
        if sku_col is None:
            raise RuntimeError("VENDAS inválido: não achei coluna de SKU.")
        df["SKU"] = df[sku_col].map(norm_sku)

        cand_qty = []
        for c in df.columns:
            cl = c.lower(); score = 0
            if "qtde" in cl: score += 3
            if "quant" in cl: score += 2
            if "venda" in cl: score += 1
            if "order" in cl: score += 1
            if score > 0: cand_qty.append((score, c))
        if not cand_qty:
            raise RuntimeError("VENDAS inválido: não achei coluna de Quantidade.")
        cand_qty.sort(reverse=True)
        qcol = cand_qty[0][1]
        df["Quantidade"] = df[qcol].map(br_to_float).fillna(0).astype(int)
        return df[["SKU","Quantidade"]].copy()

    raise RuntimeError("Tipo de arquivo desconhecido.")

# ===================== KITS (EXPLOSÃO) =====================
def explodir_por_kits(df: pd.DataFrame, kits: pd.DataFrame, sku_col: str, qtd_col: str) -> pd.DataFrame:
    base = df.copy()
    base["kit_sku"] = base[sku_col].map(norm_sku)
    base["qtd"]     = base[qtd_col].astype(int)
    merged   = base.merge(kits, on="kit_sku", how="left")
    exploded = merged.dropna(subset=["component_sku"]).copy()
    exploded["qty"] = exploded["qty"].astype(int)
    exploded["quantidade_comp"] = exploded["qtd"] * exploded["qty"]
    out = exploded.groupby("component_sku", as_index=False)["quantidade_comp"].sum()
    out = out.rename(columns={"component_sku":"SKU","quantidade_comp":"Quantidade"})
    return out

# ===================== COMPRA AUTOMÁTICA (LÓGICA ORIGINAL) =====================
def calcular(full_df, fisico_df, vendas_df, cat: Catalogo, h=60, g=0.0, LT=0):
    kits = construir_kits_efetivo(cat)
    full = full_df.copy()
    full["SKU"] = full["SKU"].map(norm_sku)
    full["Vendas_Qtd_60d"] = full["Vendas_Qtd_60d"].astype(int)
    full["Estoque_Full"]   = full["Estoque_Full"].astype(int)
    full["Em_Transito"]    = full["Em_Transito"].astype(int) 

    shp = vendas_df.copy()
    shp["SKU"] = shp["SKU"].map(norm_sku)
    shp["Quantidade_60d"] = shp["Quantidade"].astype(int)

    # 1. Explode Vendas de FULL/Shopee para nível componente
    ml_comp = explodir_por_kits(
        full[["SKU","Vendas_Qtd_60d"]].rename(columns={"SKU":"kit_sku","Vendas_Qtd_60d":"Qtd"}),
        kits,"kit_sku","Qtd").rename(columns={"Quantidade":"ML_60d"})
    shopee_comp = explodir_por_kits(
        shp[["SKU","Quantidade_60d"]].rename(columns={"SKU":"kit_sku","Quantidade_60d":"Qtd"}),
        kits,"kit_sku","Qtd").rename(columns={"Quantidade":"Shopee_60d"})

    cat_df = cat.catalogo_simples[["component_sku","fornecedor","status_reposicao"]].rename(columns={"component_sku":"SKU"})

    # 2. Mescla Catálogo com Demandas (apenas SKUs do catálogo que DEVEM ser repostos)
    demanda = cat_df.merge(ml_comp, on="SKU", how="left").merge(shopee_comp, on="SKU", how="left")
    demanda[["ML_60d","Shopee_60d"]] = demanda[["ML_60d","Shopee_60d"]].fillna(0).astype(int)
    demanda["TOTAL_60d"] = np.maximum(demanda["ML_60d"] + demanda["Shopee_60d"], demanda["ML_60d"]).astype(int)
    demanda["Vendas_Total_60d"] = demanda["ML_60d"] + demanda["Shopee_60d"] 

    fis = fisico_df.copy()
    fis["SKU"] = fis["SKU"].map(norm_sku)
    fis["Estoque_Fisico"] = fis["Estoque_Fisico"].fillna(0).astype(int)
    fis["Preco"] = fis["Preco"].fillna(0.0)

    # 3. Mescla com Estoque Físico e FULL
    base = demanda.merge(fis, on="SKU", how="left")
    base["Estoque_Fisico"] = base["Estoque_Fisico"].fillna(0).astype(int)
    base["Preco"] = base["Preco"].fillna(0.0)
    
    # FIX V3.0/V3.1: Merge com Full. Garantir que todas as colunas sejam mantidas e preenchidas
    # Cria um DF de full simplificado para merge
    full_simple = full[["SKU", "Estoque_Full", "Em_Transito"]].copy()
    
    base = base.merge(full_simple, on="SKU", how="left", suffixes=('_base', '_full'))
    
    # Se merge do Full falhar, preenche com 0.
    base["Estoque_Full"] = base["Estoque_Full"].fillna(0).astype(int)
    base["Em_Transito"] = base["Em_Transito"].fillna(0).astype(int) 
    # Remove qualquer coluna extra de merge, garantindo que as que precisam ser exibidas estejam lá
    base = base.drop(columns=[col for col in base.columns if col.endswith('_full') or col.endswith('_base')], errors='ignore')

    
    # 4. Cálculo de Necessidade (Target)
    fator = (1.0 + g/100.0) ** (h/30.0)
    fk = full.copy()
    fk["vendas_dia"] = fk["Vendas_Qtd_60d"] / 60.0
    fk["alvo"] = np.round(fk["vendas_dia"] * (LT + h) * fator).astype(int)
    fk["oferta"] = (full["Estoque_Full"] + full["Em_Transito"]).astype(int) # Usar full_df original (que já tem as colunas garantidas)
    fk["envio_desejado"] = (fk["alvo"] - fk["oferta"]).clip(lower=0).astype(int)

    necessidade = explodir_por_kits(
        fk[["SKU","envio_desejado"]].rename(columns={"SKU":"kit_sku","envio_desejado":"Qtd"}),
        kits,"kit_sku","Qtd").rename(columns={"Quantidade":"Necessidade"})

    base = base.merge(necessidade, on="SKU", how="left")
    base["Necessidade"] = base["Necessidade"].fillna(0).astype(int)

    base["Demanda_dia"]  = base["TOTAL_60d"] / 60.0
    base["Reserva_30d"]  = np.round(base["Demanda_dia"] * 30).astype(int)
    base["Folga_Fisico"] = (base["Estoque_Fisico"] - base["Reserva_30d"]).clip(lower=0).astype(int)

    base["Compra_Sugerida"] = (base["Necessidade"] - base["Folga_Fisico"]).clip(lower=0).astype(int)

    base["Valor_Compra_R$"] = (base["Compra_Sugerida"].astype(float) * base["Preco"].astype(float)).round(2)
    
    # ATENÇÃO: Seleção das colunas finais
    df_final = base[[
        "SKU","fornecedor",
        "Vendas_Total_60d",
        "Estoque_Full",
        "Estoque_Fisico","Preco","Compra_Sugerida","Valor_Compra_R$",
        "ML_60d","Shopee_60d","TOTAL_60d","Reserva_30d","Folga_Fisico","Necessidade", "Em_Transito"
    ]].reset_index(drop=True)

    # Painel (mantido o original para métricas)
    fis_unid  = int(fis["Estoque_Fisico"].sum())
    fis_valor = float((fis["Estoque_Fisico"] * fis["Preco"]).sum())
    full_stock_comp = explodir_por_kits(
        full[["SKU","Estoque_Full"]].rename(columns={"SKU":"kit_sku","Estoque_Full":"Qtd"}),
        kits,"kit_sku","Qtd")
    full_stock_comp = full_stock_comp.merge(fis[["SKU","Preco"]], on="SKU", how="left")
    full_unid  = int(full["Estoque_Full"].sum())
    full_valor = float((full_stock_comp["Quantidade"].fillna(0) * full_stock_comp["Preco"].fillna(0.0)).sum())

    painel = {"full_unid": full_unid, "full_valor": full_valor, "fisico_unid": fis_unid, "fisico_valor": fisico_valor}
    return df_final, painel

# ===================== EXPORT CSV / STYLER =====================
def exportar_carrinho_csv(df: pd.DataFrame) -> bytes:
    df["Data_Hora_OC"] = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return df.to_csv(index=False).encode("utf-8")

# FUNÇÕES CUSTOMIZADAS DE FORMATO (MAIOR ESTABILIDADE)
def format_br_float(x):
    if pd.isna(x): return '-'
    # Formata como float com 2 casas e troca os separadores (1,000.50 -> 1.000,50)
    return f"{x:,.2f}".replace('.', 'TEMP').replace(',', '.').replace('TEMP', ',')

def format_br_currency(x):
    if pd.isna(x): return '-'
    # Formata como moeda BR
    return f"R$ {format_br_float(x)}"

def format_br_int(x):
    if pd.isna(x): return '-'
    # Formata como inteiro e troca os separadores (1,000 -> 1.000)
    return f"{x:,.0f}".replace('.', 'TEMP').replace(',', '.').replace('TEMP', ',')

def style_df_compra(df: pd.DataFrame):
    """Aplica o destaque na coluna Compra_Sugerida e formata valores usando funções lambda."""
    
    # NOVO: USANDO LAMBDAS PARA FORÇAR FORMATO E RESOLVER O VALUERROR
    format_mapping = {
        'Estoque_Fisico': lambda x: format_br_int(x),
        'Compra_Sugerida': lambda x: format_br_int(x),
        'Vendas_Total_60d': lambda x: format_br_int(x),
        'Estoque_Full': lambda x: format_br_int(x),
        'Em_Transito': lambda x: format_br_int(x), 
        'Preco': lambda x: format_br_currency(x),
        'Valor_Compra_R$': lambda x: format_br_currency(x),
        'Qtd_Ajustada': lambda x: format_br_int(x),
        'Preco_Custo': lambda x: format_br_currency(x),
        'Valor_Ajustado_R$': lambda x: format_br_currency(x),
        'Valor_Sugerido_R$': lambda x: format_br_currency(x),
    }
    
    styler = df.style.format({c: fmt for c, fmt in format_mapping.items() if c in df.columns})
    
    # Aplica cor de fundo se Compra_Sugerida for > 0
    def highlight_compra(s):
        is_compra = s.name == 'Compra_Sugerida' or s.name == 'Qtd_Ajustada'
        
        # Garante que s é numérico antes de comparar (CORREÇÃO DE ERRO)
        s_numeric = pd.to_numeric(s, errors='coerce').fillna(0)
        
        if is_compra:
            return ['background-color: #A93226; color: white' if v > 0 else '' for v in s_numeric]
        return ['' for _ in s]

    # Aplica o destaque
    if 'Compra_Sugerida' in df.columns:
        styler = styler.apply(highlight_compra, axis=0, subset=['Compra_Sugerida'])
    if 'Qtd_Ajustada' in df.columns:
        styler = styler.apply(highlight_compra, axis=0, subset=['Qtd_Ajustada'])
    
    return styler

# ===================== UI: SIDEBAR (PADRÃO) =====================
with st.sidebar:
    st.subheader("Parâmetros")
    h  = st.selectbox("Horizonte (dias)", [30, 60, 90], index=1, key="param_h")
    g  = st.number_input("Crescimento % ao mês", value=0.0, step=1.0, key="param_g")
    LT = st.number_input("Lead time (dias)", value=0, step=1, min_value=0, key="param_lt")

    st.markdown("---")
    st.subheader("Padrão (KITS/CAT)")
    st.caption(f"Procura por **{LOCAL_PADRAO_FILENAME}** localmente ou baixa do Sheets.")
    
    
    # Status de carregamento do Padrão
    if st.session_state.loaded_at:
        origem = st.session_state.loaded_at.split(' ')[-1]
        st.caption(f"Padrão carregado em: {st.session_state.loaded_at.split(' ')[0]} - (Origem: **{origem}**)")
    
    colA, colB = st.columns([1, 1])
    with colA:
        if st.button("Carregar Padrão agora", use_container_width=True):
            try:
                # NOVO V3.2: Chama a função que prioriza o arquivo local
                cat, origem = carregar_padrao_local_ou_sheets(DEFAULT_SHEET_LINK)
                
                st.session_state.catalogo_df = cat.catalogo_simples.rename(columns={"sku":"component_sku"})
                st.session_state.kits_df = cat.kits_reais
                st.session_state.loaded_at = dt.datetime.now().strftime(f"%Y-%m-%d %H:%M:%S {origem}")
                st.success(f"Padrão carregado com sucesso (Origem: {origem}).")
            except Exception as e:
                st.session_state.catalogo_df = None; st.session_state.kits_df = None; st.session_state.loaded_at = None
                st.error(str(e))
    with colB:
        st.link_button("🔗 Abrir no Drive (editar)", DEFAULT_SHEET_LINK, use_container_width=True)

    st.text_input("Link alternativo do Google Sheets (opcional)", key="alt_sheet_link",
                  help="Se necessário, cole o link e use o botão abaixo.")
    if st.button("Carregar deste link alternativo", use_container_width=True):
        try:
            # Mantém a lógica de carregamento do link alternativo separada
            cat = carregar_padrao_do_link(st.session_state.alt_sheet_link.strip())
            st.session_state.catalogo_df = cat.catalogo_simples.rename(columns={"sku":"component_sku"})
            st.session_state.kits_df = cat.kits_reais
            st.session_state.loaded_at = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S alt_sheets")
            st.success("Padrão carregado (link alternativo).")
        except Exception as e:
            st.session_state.catalogo_df = None; st.session_state.kits_df = None; st.session_state.loaded_at = None
            st.error(str(e))

# ===================== TÍTULO =====================
st.title("Reposição Logística — Alivvia")
if st.session_state.catalogo_df is None or st.session_state.kits_df is None:
    st.warning("► Carregue o **Padrão (KITS/CAT)** no sidebar antes de usar as abas.")

# ===================== ABAS (NOVA ESTRUTURA) =====================
tab1, tab2, tab3, tab4 = st.tabs([
    "📂 Dados das Empresas", 
    "🔍 Análise de Compra (Consolidado)", 
    "🛒 Pedido de Compra",
    "📦 Alocação de Compra"
])

# ---------- TAB 1: UPLOADS ----------
with tab1:
    st.subheader("Uploads fixos por empresa (mantidos no cache de disco)")
    st.caption("Ao fazer upload, o arquivo é salvo no cache do servidor para persistir reinicializações.")

    def bloco_empresa(emp: str):
        st.markdown(f"### {emp}")
        c1, c2 = st.columns(2)
        
        # Funções para upload e persistência
        def handle_upload(up_file, file_type):
            if up_file is not None:
                up_bytes = up_file.read()
                path_bin = get_local_file_path(emp, file_type)
                path_name = get_local_name_path(emp, file_type)
                
                # Salva no disco (bytes e nome)
                with open(path_bin, 'wb') as f_bin:
                    f_bin.write(up_bytes)
                with open(path_name, 'w', encoding='utf-8') as f_name:
                    f_name.write(up_file.name)
                    
                # Salva metadados na sessão
                st.session_state[emp][file_type]["name"]  = up_file.name
                st.session_state[emp][file_type]["bytes"] = up_bytes
                st.session_state[emp][file_type]['is_cached'] = True
                st.success(f"{file_type} carregado e salvo: {up_file.name}")
        
        def display_status(file_type):
            if st.session_state[emp][file_type]["name"]:
                status = "✅ No disco (persiste)" if st.session_state[emp][file_type].get('is_cached') else "(Na memória, temporário)"
                st.caption(f"{file_type} salvo: **{st.session_state[emp][file_type]['name']}** {status}")

        # FULL
        with c1:
            st.markdown(f"**FULL — {emp}**")
            up_full = st.file_uploader("CSV/XLSX/XLS", type=["csv","xlsx","xls"], key=f"up_full_{emp}")
            handle_upload(up_full, "FULL")
            display_status("FULL")

        # Shopee/MT
        with c2:
            st.markdown(f"**Shopee/MT — {emp}**")
            up_v = st.file_uploader("CSV/XLSX/XLS", type=["csv","xlsx","xls"], key=f"up_v_{emp}")
            handle_upload(up_v, "VENDAS")
            display_status("VENDAS")

        # Estoque Físico
        st.markdown("**Estoque Físico — opcional (necessário só para Compra Automática)**")
        up_e = st.file_uploader("CSV/XLSX/XLS", type=["csv","xlsx","xls"], key=f"up_e_{emp}")
        # CORREÇÃO DE INDENTAÇÃO (LINHA 753)
        handle_upload(up_e, "ESTOQUE")
        display_status("ESTOQUE")

        c3, c4 = st.columns([1,1])
        with c3:
            # Botão Salvar (apenas um feedback, pois o salvamento é automático no upload)
            st.button(f"Salvar {emp} (Uploads Persistem)", use_container_width=True, key=f"save_{emp}")
        with c4:
            if st.button(f"Limpar {emp} e Cache", use_container_width=True, key=f"clr_{emp}"):
                # Limpa os arquivos do disco
                for file_type in ["FULL", "VENDAS", "ESTOQUE"]:
                    if os.path.exists(get_local_file_path(emp, file_type)):
                        os.remove(get_local_file_path(emp, file_type))
                    if os.path.exists(get_local_name_path(emp, file_type)):
                        os.remove(get_local_name_path(emp, file_type))
                
                # Limpa a sessão
                st.session_state[emp] = {"FULL":{"name":None,"bytes":None},
                                         "VENDAS":{"name":None,"bytes":None},
                                         "ESTOQUE":{"name":None,"bytes":None}}
                st.session_state[f"resultado_{emp}"] = None
                st.info(f"{emp} limpo e cache de disco apagado.")

        st.divider()

    bloco_empresa("ALIVVIA")
    bloco_empresa("JCA")

# ---------- TAB 2: ANÁLISE DE COMPRA (CONSOLIDADO) ----------
with tab2:
    st.subheader("Gerar e Analisar Compra (Consolidado) — Lógica Original")

    if st.session_state.catalogo_df is None or st.session_state.kits_df is None:
        st.info("Carregue o **Padrão (KITS/CAT)** no sidebar.")
    else:
        
        # --- Cálculo/Persistência ---
        def run_calculo(empresa: str):
            dados = st.session_state[empresa]
            try:
                # valida presença
                for k, rot in [("FULL","FULL"),("VENDAS","Shopee/MT"),("ESTOQUE","Estoque")]:
                    if not (dados[k]["name"] and dados[k]["bytes"]):
                        raise RuntimeError(f"Arquivo '{rot}' não foi salvo para {empresa}. Vá em **Dados das Empresas** e salve.")

                # leitura pelos BYTES
                full_raw   = load_any_table_from_bytes(dados["FULL"]["name"], dados["FULL"]["bytes"])
                vendas_raw = load_any_table_from_bytes(dados["VENDAS"]["name"], dados["VENDAS"]["bytes"])
                fisico_raw = load_any_table_from_bytes(dados["ESTOQUE"]["name"], dados["ESTOQUE"]["bytes"])

                # tipagem
                t_full = mapear_tipo(full_raw)
                t_v    = mapear_tipo(vendas_raw)
                t_f    = mapear_tipo(fisico_raw)
                if t_full != "FULL":   raise RuntimeError("FULL inválido: precisa de SKU e Vendas_60d/Estoque_full.")
                if t_v    != "VENDAS": raise RuntimeError("Vendas inválido: não achei coluna de quantidade.")
                if t_f    != "FISICO": raise RuntimeError("Estoque inválido: precisa de Estoque e Preço.")

                full_df   = mapear_colunas(full_raw, t_full)
                vendas_df = mapear_colunas(vendas_raw, t_v)
                fisico_df = mapear_colunas(fisico_raw, t_f)

                cat = Catalogo(
                    catalogo_simples=st.session_state.catalogo_df.rename(columns={"sku":"component_sku"}),
                    kits_reais=st.session_state.kits_df
                )
                df_final, painel = calcular(full_df, fisico_df, vendas_df, cat, h=st.session_state.param_h, g=st.session_state.param_g, LT=st.session_state.param_lt)
                
                # NOVO: Persiste o resultado
                st.session_state[f"resultado_{empresa}"] = df_final
                st.success(f"Cálculo para {empresa} concluído.")
                
            except Exception as e:
                st.error(f"Erro ao calcular {empresa}: {str(e)}")

        colC, colD = st.columns(2)
        with colC:
            if st.button("Gerar Compra — ALIVVIA", type="primary"):
                run_calculo("ALIVVIA")
        with colD:
            if st.button("Gerar Compra — JCA", type="primary"):
                run_calculo("JCA")

        # --- Filtros e Visualização ---
        st.markdown("---")
        st.subheader("Filtros de Análise (Aplicado em Ambas Empresas)")
        
        df_A = st.session_state.resultado_ALIVVIA
        df_J = st.session_state.resultado_JCA
        
        if df_A is None and df_J is None:
            st.info("Gere o cálculo para pelo menos uma empresa acima para visualizar e filtrar.")
        else:
            df_full = pd.concat([df for df in [df_A, df_J] if df is not None], ignore_index=True)
            
            # Filtros dinâmicos
            c1, c2 = st.columns(2)
            with c1:
                sku_filter = st.text_input("Filtro por SKU (contém)", key="filt_sku").upper().strip()
            with c2:
                fornecedor_opc = df_full["fornecedor"].unique().tolist() if df_full is not None else []
                fornecedor_opc.insert(0, "TODOS")
                fornecedor_filter = st.selectbox("Filtro por Fornecedor", fornecedor_opc, key="filt_forn")
            
            # Aplica filtros
            def aplicar_filtro(df: pd.DataFrame) -> pd.DataFrame:
                if df is None: return None
                df_filt = df.copy()
                if sku_filter:
                    df_filt = df_filt[df_filt["SKU"].str.contains(sku_filter, na=False)]
                if fornecedor_filter != "TODOS":
                    df_filt = df_filt[df_filt["fornecedor"] == fornecedor_filter]
                return df_filt

            df_A_filt = aplicar_filtro(df_A)
            df_J_filt = aplicar_filtro(df_J)

            # --- NOVA LÓGICA DE SELEÇÃO POR TEXTO (V3.2.4) ---
            st.markdown("---")
            st.subheader("🛒 Seleção de Itens para Compra (Cole/Digite SKUs)")
            
            # Campo de entrada de SKUs
            st.text_area(
                "Cole os SKUs (separados por vírgula, espaço ou linha) que deseja adicionar ao carrinho:",
                key="selected_skus_input",
                height=100,
                help="Os SKUs serão validados contra os resultados do cálculo (Comprado Sugerida > 0)."
            )

            if st.button("✅ Adicionar SKUs do Lote ao Pedido", type="secondary"):
                
                # Processa a string de input
                input_skus_raw = st.session_state.selected_skus_input
                if not input_skus_raw:
                    st.warning("Nenhum SKU foi digitado/colado.")
                    
                # Normaliza e pega os SKUs únicos
                input_skus = {norm_sku(s) for s in re.split(r'[,\s\n]+', input_skus_raw) if s.strip()}
                
                if not input_skus:
                    st.warning("Nenhum SKU válido foi detectado na entrada.")
                else:
                    carrinho = []
                    
                    # 1. Processa ALIVVIA: Filtra resultados por SKUs digitados
                    if df_A is not None:
                        selec_A = df_A[df_A["SKU"].isin(input_skus)].copy()
                        selec_A = selec_A[selec_A["Compra_Sugerida"] > 0].copy()
                        if not selec_A.empty:
                            selec_A["Empresa"] = "ALIVVIA"
                            carrinho.append(selec_A)
                    
                    # 2. Processa JCA: Filtra resultados por SKUs digitados
                    if df_J is not None:
                        selec_J = df_J[df_J["SKU"].isin(input_skus)].copy()
                        selec_J = selec_J[selec_J["Compra_Sugerida"] > 0].copy()
                        if not selec_J.empty:
                            selec_J["Empresa"] = "JCA"
                            carrinho.append(selec_J)
                    
                    if carrinho:
                        # Monta o carrinho final
                        cols_carrinho = ["Empresa", "SKU", "fornecedor", "Preco", "Compra_Sugerida", "Valor_Compra_R$"]
                        carrinho_df = pd.concat(carrinho)[cols_carrinho]
                        carrinho_df.columns = ["Empresa", "SKU", "Fornecedor", "Preco_Custo", "Qtd_Sugerida", "Valor_Sugerido_R$"]
                        carrinho_df["Qtd_Ajustada"] = carrinho_df["Qtd_Sugerida"]
                        
                        # Força a tipagem antes de salvar no carrinho
                        carrinho_df = enforce_numeric_types(carrinho_df)

                        st.session_state.carrinho_compras = [carrinho_df.reset_index(drop=True)]
                        st.success(f"Adicionado {len(carrinho_df)} itens ao Pedido de Compra. Vá para a aba '🛒 Pedido de Compra' para finalizar.")
                    else:
                        st.warning("Nenhum dos SKUs digitados tinha Compra Sugerida > 0 ou pertenciam aos seus dados.")


            # --- Visualização de Resultados (Sem coluna 'Selecionar') ---
            col_order = ["SKU", "fornecedor", "Vendas_Total_60d", "Estoque_Full", "Estoque_Fisico", "Preco", "Compra_Sugerida", "Valor_Compra_R$", "Em_Transito"]
            
            if df_A_filt is not None and not df_A_filt.empty:
                st.markdown("### ALIVVIA")
                
                # Força a tipagem antes de estilizar
                df_A_filt_typed = enforce_numeric_types(df_A_filt)
                
                st.dataframe(
                    style_df_compra(df_A_filt_typed[col_order]),
                    use_container_width=True,
                    column_order=col_order,
                    key="df_view_A_final" # Novo key
                )
            else:
                 st.info("ALIVVIA: Nenhum item corresponde aos filtros.")


            if df_J_filt is not None and not df_J_filt.empty:
                st.markdown("### JCA")
                # Força a tipagem antes de estilizar
                df_J_filt_typed = enforce_numeric_types(df_J_filt)
                
                st.dataframe(
                    style_df_compra(df_J_filt_typed[col_order]),
                    use_container_width=True,
                    column_order=col_order,
                    key="df_view_J_final" # Novo key
                )
            else:
                st.info("JCA: Nenhum item corresponde aos filtros.")

# ---------- TAB 3: PEDIDO DE COMPRA ----------
with tab3:
    st.subheader("🛒 Revisão e Finalização do Pedido de Compra")
    
    if not st.session_state.carrinho_compras:
        st.info("O carrinho de compras está vazio. Adicione itens na aba **Análise de Compra (Consolidado)**.")
    else:
        # Puxa o carrinho, que já está com tipos forçados
        df_carrinho = st.session_state.carrinho_compras[0].copy()
        
        # Auditoria/Detalhes da OC
        st.markdown("---")
        c1, c2 = st.columns(2)
        with c1:
            st.text_input("Fornecedor Principal do Pedido:", key="oc_fornecedor", value=df_carrinho["Fornecedor"].iloc[0] if not df_carrinho.empty else "")
        with c2:
            st.text_input("Número da Ordem de Compra (OC):", key="oc_num")
        st.text_area("Nota/Observação:", key="oc_obs")
        st.markdown("---")

        st.markdown("### Ajuste de Quantidades")
        
        # Configura a coluna Qtd_Ajustada para ser editável (inteiro > 0)
        col_config = {
            "Qtd_Ajustada": st.column_config.NumberColumn(
                "Qtd. Ajustada (Final)",
                help="Quantidade final para compra.",
                min_value=1,
                format="%d",
                default=1
            )
        }
        
        # Exibe o editor de dados
        edited_carrinho = st.data_editor(
            style_df_compra(df_carrinho), # Usa a função de estilo na edição
            use_container_width=True,
            column_config=col_config,
            disabled=["Empresa", "SKU", "Fornecedor", "Preco_Custo", "Qtd_Sugerida", "Valor_Sugerido_R$"]
        )
        
        # Recalcula o valor total com a quantidade ajustada (já são float devido ao enforce_numeric_types)
        edited_carrinho["Valor_Ajustado_R$"] = (edited_carrinho["Qtd_Ajustada"] * edited_carrinho["Preco_Custo"]).round(2)
        
        # Atualiza o estado para persistir as alterações
        st.session_state.carrinho_compras[0] = edited_carrinho

        # Métricas Finais
        total_unidades = int(edited_carrinho["Qtd_Ajustada"].sum())
        total_valor_oc = float(edited_carrinho["Valor_Ajustado_R$"].sum())
        
        c3, c4 = st.columns(2)
        c3.metric("Total de Itens", f"{len(edited_carrinho)}")
        c4.metric("Valor Total do Pedido", f"R$ {total_valor_oc:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))

        st.markdown("---")
        
        # Botão de Exportação Final (CSV)
        if st.button("📥 Exportar Pedido Final (CSV)", type="primary"):
            df_export = edited_carrinho.copy()
            # Adiciona colunas de auditoria
            df_export["OC_Fornecedor"] = st.session_state.oc_fornecedor
            df_export["OC_Numero"] = st.session_state.oc_num
            df_export["OC_Obs"] = st.session_state.oc_obs
            
            csv = exportar_carrinho_csv(df_export)
            st.download_button(
                "Baixar CSV para Ordem de Compra",
                data=csv,
                file_name=f"OC_{st.session_state.oc_fornecedor.replace(' ', '_')}_{dt.datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv"
            )
            st.success("CSV gerado! Use este arquivo para alimentar o seu relatório de Ordem de Compra no Google Looker Studio.")

# ---------- TAB 4: ALOCAÇÃO DE COMPRA (sem estoque) ----------
with tab4:
    st.subheader("Distribuir quantidade entre empresas — proporcional às vendas (FULL + Shopee)")

    if st.session_state.catalogo_df is None or st.session_state.kits_df is None:
        st.info("Carregue o **Padrão (KITS/CAT)** no sidebar.")
    else:
        CATALOGO = st.session_state.catalogo_df
        sku_opcoes = CATALOGO["sku"].dropna().astype(str).sort_values().unique().tolist()
        sku_escolhido = st.selectbox("SKU do componente para alocar", sku_opcoes, key="alloc_sku")
        qtd_lote = st.number_input("Quantidade total do lote (ex.: 400)", min_value=1, value=1000, step=50, key="alloc_qtd")

        if st.button("Calcular alocação proporcional"):
            try:
                # precisa de FULL e VENDAS salvos para AMBAS as empresas
                missing = []
                for emp in ["ALIVVIA","JCA"]:
                    if not (st.session_state[emp]["FULL"]["name"] and st.session_state[emp]["FULL"]["bytes"]):
                        missing.append(f"{emp} FULL")
                    if not (st.session_state[emp]["VENDAS"]["name"] and st.session_state[emp]["VENDAS"]["bytes"]):
                        missing.append(f"{emp} Shopee/MT")
                if missing:
                    raise RuntimeError("Faltam arquivos salvos: " + ", ".join(missing) + ". Use a aba **Dados das Empresas**.")

                # leitura BYTES
                def read_pair(emp: str) -> Tuple[pd.DataFrame,pd.DataFrame]:
                    fa = load_any_table_from_bytes(st.session_state[emp]["FULL"]["name"],   st.session_state[emp]["FULL"]["bytes"])
                    sa = load_any_table_from_bytes(st.session_state[emp]["VENDAS"]["name"], st.session_state[emp]["VENDAS"]["bytes"])
                    tfa = mapear_tipo(fa); tsa = mapear_tipo(sa)
                    if tfa != "FULL":   raise RuntimeError(f"FULL inválido ({emp}): precisa de SKU e Vendas_60d/Estoque_full.")
                    if tsa != "VENDAS": raise RuntimeError(f"Vendas inválido ({emp}): não achei coluna de quantidade.")
                    return mapear_colunas(fa, tfa), mapear_colunas(sa, tsa)

                full_A, shp_A = read_pair("ALIVVIA")
                full_J, shp_J = read_pair("JCA")

                # explode por kits --> demanda 60d por componente
                cat = Catalogo(
                    catalogo_simples=CATALOGO.rename(columns={"sku":"component_sku"}),
                    kits_reais=st.session_state.kits_df
                )
                kits = construir_kits_efetivo(cat)

                def vendas_componente(full_df, shp_df) -> pd.DataFrame:
                    a = explodir_por_kits(full_df[["SKU","Vendas_Qtd_60d"]].rename(columns={"SKU":"kit_sku","Vendas_Qtd_60d":"Qtd"}), kits,"kit_sku","Qtd")
                    a = a.rename(columns={"Quantidade":"ML_60d"})
                    b = explodir_por_kits(shp_df[["SKU","Quantidade"]].rename(columns={"SKU":"kit_sku","Quantidade":"Qtd"}), kits,"kit_sku","Qtd")
                    b = b.rename(columns={"Quantidade":"Shopee_60d"})
                    out = pd.merge(a, b, on="SKU", how="outer").fillna(0)
                    out["Demanda_60d"] = out["ML_60d"].astype(int) + out["Shopee_60d"].astype(int)
                    return out[["SKU","Demanda_60d"]]

                demA = vendas_componente(full_A, shp_A)
                demJ = vendas_componente(full_J, shp_J)

                dA = int(demA.loc[demA["SKU"]==norm_sku(sku_escolhido), "Demanda_60d"].sum())
                dJ = int(demJ.loc[demJ["SKU"]==norm_sku(sku_escolhido), "Demanda_60d"].sum())

                total = dA + dJ
                if total == 0:
                    st.warning("Sem vendas detectadas; alocação 50/50 por falta de base.")
                    propA = propJ = 0.5
                else:
                    propA = dA / total
                    propJ = dJ / total

                alocA = int(round(qtd_lote * propA))
                alocJ = int(qtd_lote - alocA)

                res = pd.DataFrame([
                    {"Empresa":"ALIVVIA", "SKU":norm_sku(sku_escolhido), "Demanda_60d":dA, "Proporção":round(propA,4), "Alocação_Sugerida":alocA},
                    {"Empresa":"JCA",     "SKU":norm_sku(sku_escolhido), "Demanda_60d":dJ, "Proporção":round(propJ,4), "Alocação_Sugerida":alocJ},
                ])
                st.dataframe(res, use_container_width=True)
                st.success(f"Total alocado: {qtd_lote} un (ALIVVIA {alocA} | JCA {alocJ})")
                st.download_button("Baixar alocação (.csv)", data=res.to_csv(index=False).encode("utf-8"),
                                   file_name=f"Alocacao_{sku_escolhido}_{qtd_lote}.csv", mime="text/csv")
            except Exception as e:
                st.error(str(e))

st.caption("© Alivvia — simples, robusto e auditável. Arquitetura V3.2.4 (FIX ESTADO: SELEÇÃO POR LISTA DE SKUS)")