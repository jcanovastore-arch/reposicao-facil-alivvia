# reposicao_facil.py
# Reposição Logística — Alivvia (Streamlit)
# - Padrão (KITS/CAT) só carrega ao clicar (nada automático).
# - Uploads por empresa com PERSISTÊNCIA EM DISCO (mantidos até você limpar).
# - Compra Automática usa apenas os arquivos salvos.
# - Alocação proporcional usa vendas (FULL + Shopee) explodidas em KITS.

import io, os, re, json, hashlib, datetime as dt
from dataclasses import dataclass
from typing import Optional, Tuple

import numpy as np
import pandas as pd
from unidecode import unidecode
import streamlit as st
import requests
from requests.adapters import HTTPAdapter, Retry

# ======= CONFIG GOOGLE SHEETS (fixo) =======
DEFAULT_SHEET_LINK = (
    "https://docs.google.com/spreadsheets/d/1cTLARjq-B5g50dL6tcntg7lb_Iu0ta43/"
    "edit?usp=sharing&ouid=109458533144345974874&rtpof=true&sd=true"
)
DEFAULT_SHEET_ID = "1cTLARjq-B5g50dL6tcntg7lb_Iu0ta43"

# ======= PERSISTÊNCIA EM DISCO =======
DATA_DIR = ".alivvia_cache"
os.makedirs(DATA_DIR, exist_ok=True)

def _company_dir(company: str) -> str:
    d = os.path.join(DATA_DIR, company.upper())
    os.makedirs(d, exist_ok=True)
    return d

def _entry_path(company: str, kind: str) -> str:
    # kind: FULL | VENDAS | ESTOQUE
    return os.path.join(_company_dir(company), f"{kind}.bin")

def _meta_path(company: str) -> str:
    return os.path.join(_company_dir(company), "_meta.json")

def save_upload(company: str, kind: str, uploaded_file) -> None:
    """
    Salva bytes + nome + timestamp. Mantém até limpar ou redeploy.
    """
    if uploaded_file is None:
        raise RuntimeError(f"Envie o arquivo para {company} / {kind} antes de salvar.")
    uploaded_file.seek(0)
    b = uploaded_file.read()
    with open(_entry_path(company, kind), "wb") as f:
        f.write(b)
    meta = load_meta(company)
    meta[kind] = {"name": uploaded_file.name, "ts": dt.datetime.now().strftime("%Y-%m-%d %H:%M")}
    with open(_meta_path(company), "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

def load_entry(company: str, kind: str) -> Optional[dict]:
    p = _entry_path(company, kind)
    if not os.path.exists(p):
        return None
    with open(p, "rb") as f:
        b = f.read()
    meta = load_meta(company).get(kind, {})
    return {"name": meta.get("name", f"{kind}.bin"), "bytes": b, "ts": meta.get("ts","")}

def load_meta(company: str) -> dict:
    p = _meta_path(company)
    if not os.path.exists(p):
        return {}
    try:
        return json.load(open(p, "r", encoding="utf-8"))
    except Exception:
        return {}

def clear_company(company: str) -> None:
    for k in ["FULL","VENDAS","ESTOQUE"]:
        p = _entry_path(company, k)
        if os.path.exists(p):
            os.remove(p)
    if os.path.exists(_meta_path(company)):
        os.remove(_meta_path(company))

def read_df_from_entry(entry: dict) -> pd.DataFrame:
    raw = io.BytesIO(entry["bytes"])
    fn  = entry["name"].lower()
    if fn.endswith(".csv"):
        return pd.read_csv(raw, dtype=str, keep_default_na=False, sep=None, engine="python")
    else:
        return pd.read_excel(raw, dtype=str, keep_default_na=False)

# ======= STREAMLIT =======
st.set_page_config(page_title="Reposição Logística — Alivvia", layout="wide")
st.title("Reposição Logística — Alivvia")

# Estado menor
for k, v in {
    "catalogo_df": None, "kits_df": None, "loaded_at": None,
    "alt_sheet_link": DEFAULT_SHEET_LINK
}.items():
    st.session_state.setdefault(k, v)

# ======= HTTP =======
def _requests_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(total=3, backoff_factor=0.6, status_forcelist=[429,500,502,503,504], allowed_methods=["GET"])
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.headers.update({"User-Agent":"Mozilla/5.0"})
    return s

def gs_export_xlsx_url(sheet_id: str) -> str:
    return f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=xlsx"

def extract_sheet_id_from_url(url: str) -> Optional[str]:
    m = re.search(r"/d/([a-zA-Z0-9\-_]+)/", url or "")
    return m.group(1) if m else None

def baixar_xlsx_do_sheets(sheet_id: str) -> bytes:
    url = gs_export_xlsx_url(sheet_id)
    r = _requests_session().get(url, timeout=30)
    try:
        r.raise_for_status()
    except requests.HTTPError as e:
        sc = e.response.status_code if e.response else "?"
        raise RuntimeError(f"Falha ao baixar XLSX (HTTP {sc}). Verifique permissão 'Qualquer pessoa com o link' e teste em janela anônima.\n{url}")
    return r.content

def baixar_xlsx_por_link_google(url: str) -> bytes:
    if "export?format=xlsx" in url:
        r = _requests_session().get(url, timeout=30); r.raise_for_status(); return r.content
    sid = extract_sheet_id_from_url(url)
    if not sid:
        raise RuntimeError("Link inválido do Google Sheets (não achei /d/<ID>/).")
    return baixar_xlsx_do_sheets(sid)

# ======= Utils =======
def norm_header(s: str) -> str:
    s = unidecode((s or "").strip().lower())
    for ch in [" ", "-", "(",")","/","\\","[","]",".",",",";",":"]:
        s = s.replace(ch, "_")
    while "__" in s: s = s.replace("__","_")
    return s.strip("_")

def br_to_float(x):
    if pd.isna(x): return np.nan
    if isinstance(x,(int,float,np.integer,np.floating)): return float(x)
    s = str(x).strip()
    if s=="": return np.nan
    s = s.replace("\u00a0"," ").replace("R$","").replace(" ","").replace(".","").replace(",",".")
    try: return float(s)
    except: return np.nan

def norm_sku(x: str) -> str:
    if pd.isna(x): return ""
    return unidecode(str(x)).strip().upper()

@dataclass
class Catalogo:
    catalogo_simples: pd.DataFrame  # component_sku, fornecedor, status_reposicao
    kits_reais: pd.DataFrame        # kit_sku, component_sku, qty

def normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy(); df.columns = [norm_header(c) for c in df.columns]; return df

# ======= Padrão (KITS/CAT) via XLSX inteiro =======
def _carregar_padrao_de_content(content: bytes) -> Catalogo:
    xls = pd.ExcelFile(io.BytesIO(content))
    def load_sheet(opts):
        for n in opts:
            if n in xls.sheet_names:
                return pd.read_excel(xls, n, dtype=str, keep_default_na=False)
        raise RuntimeError(f"Aba não encontrada. Esperado: {opts}. Abas existentes: {xls.sheet_names}")

    df_kits = load_sheet(["KITS","KITS_REAIS","kits","kits_reais"])
    df_cat  = load_sheet(["CATALOGO_SIMPLES","CATALOGO","catalogo_simples","catalogo"])

    # KITS
    df_kits = normalize_cols(df_kits)
    map_k = {
        "kit_sku": ["kit_sku","kit","sku_kit"],
        "component_sku":["component_sku","componente","sku_componente","component","sku_component"],
        "qty":["qty","qty_por_kit","qtd_por_kit","quantidade_por_kit","qtd","quantidade"]
    }
    ren = {}
    for target, cands in map_k.items():
        for c in cands:
            if c in df_kits.columns:
                ren[c]=target; break
    df_kits = df_kits.rename(columns=ren)
    for c in ["kit_sku","component_sku","qty"]:
        if c not in df_kits.columns:
            raise RuntimeError("KITS precisa de kit_sku, component_sku, qty.")
    df_kits = df_kits[["kit_sku","component_sku","qty"]].copy()
    df_kits["kit_sku"] = df_kits["kit_sku"].map(norm_sku)
    df_kits["component_sku"] = df_kits["component_sku"].map(norm_sku)
    df_kits["qty"] = df_kits["qty"].map(br_to_float).fillna(0).astype(int)
    df_kits = df_kits[df_kits["qty"]>=1].drop_duplicates(["kit_sku","component_sku"])

    # CAT
    df_cat = normalize_cols(df_cat)
    ren_c = {}
    for target, cands in {
        "component_sku":["component_sku","sku","produto","item","codigo","sku_componente"],
        "fornecedor":["fornecedor","supplier","fab","marca"],
        "status_reposicao":["status_reposicao","status","reposicao_status"]
    }.items():
        for c in cands:
            if c in df_cat.columns:
                ren_c[c]=target; break
    df_cat = df_cat.rename(columns=ren_c)
    if "component_sku" not in df_cat.columns:
        raise RuntimeError("CATALOGO precisa ter 'component_sku' (ou 'sku').")
    if "fornecedor" not in df_cat.columns: df_cat["fornecedor"]=""
    if "status_reposicao" not in df_cat.columns: df_cat["status_reposicao"]=""
    df_cat["component_sku"] = df_cat["component_sku"].map(norm_sku)
    df_cat = df_cat.drop_duplicates(["component_sku"], keep="last")

    return Catalogo(catalogo_simples=df_cat, kits_reais=df_kits)

def carregar_padrao_do_xlsx(sheet_id: str) -> Catalogo:
    return _carregar_padrao_de_content(baixar_xlsx_do_sheets(sheet_id))

def carregar_padrao_do_link(url: str) -> Catalogo:
    return _carregar_padrao_de_content(baixar_xlsx_por_link_google(url))

# ======= Leitura genérica upload =======
def load_any_table(uploaded_file) -> Optional[pd.DataFrame]:
    if uploaded_file is None: return None
    name = uploaded_file.name.lower()
    try:
        if name.endswith(".csv"):
            uploaded_file.seek(0)
            df = pd.read_csv(uploaded_file, dtype=str, keep_default_na=False, sep=None, engine="python")
        else:
            uploaded_file.seek(0)
            df = pd.read_excel(uploaded_file, dtype=str, keep_default_na=False)
    except Exception as e:
        raise RuntimeError(f"Não consegui ler '{uploaded_file.name}': {e}")
    df.columns = [norm_header(c) for c in df.columns]
    # FULL com header na 3ª linha (alguns relatórios)
    if ("sku" not in df.columns) and ("codigo" not in df.columns) and ("codigo_sku" not in df.columns) and len(df)>0:
        try:
            uploaded_file.seek(0)
            df = pd.read_excel(uploaded_file, dtype=str, keep_default_na=False, header=2)
            df.columns = [norm_header(c) for c in df.columns]
        except Exception:
            pass
    sku_col = next((c for c in ["sku","codigo","codigo_sku"] if c in df.columns), None)
    if sku_col:
        df[sku_col]=df[sku_col].map(norm_sku); df=df[df[sku_col]!=""]
    for c in list(df.columns):
        df = df[~df[c].astype(str).str.contains(r"^TOTALS?$|^TOTAIS?$", case=False, na=False)]
    return df.reset_index(drop=True)

# ======= Detecção & mapeamento =======
def mapear_tipo(df: pd.DataFrame) -> str:
    cols = [c.lower() for c in df.columns]

    # SKU presente (qualquer coluna contendo "sku")
    tem_sku = any("sku" in c for c in cols) or any(c in {"sku","codigo","codigo_sku"} for c in cols)

    # FULL: aceita variações de "vendas 60d"
    def eh_vendas_60(c: str) -> bool:
        cl = c.lower().replace(" ", "").replace("__","_")
        return (
            ("venda" in cl and "60" in cl) or
            cl.startswith("vendas_60") or
            cl in {"vendas60","vendas_qtd_60","vendas_qtd_60d","vendas60d"}
        )

    tem_vendas60 = any(eh_vendas_60(c) for c in cols)
    tem_estoque_full_like = any(("estoque" in c and "full" in c) or c == "estoque_full" for c in cols)
    tem_transito_like = any(("transito" in c) or c in {
        "em_transito","em transito","em_transito_full","em_transito_do_anuncio"
    } for c in cols)

    # FÍSICO: agora basta ter estoque; preço é opcional (vamos assumir 0 se faltar)
    tem_estoque_generico = any(("estoque" in c) or c in {"estoque_atual","qtd","quantidade"} for c in cols)

    # VENDAS genéricas (Shopee/MT): quantidade/ordens/etc., sem preço
    tem_preco = any(c in {"preco","preço","preco_compra","preco_medio","preco_unitario","custo","custo_medio"} for c in cols)
    tem_qtd_livre = any(any(k in c for k in ["qtde","quant","venda","orders","pedido","sold","ordens","unid"]) for c in cols)

    if tem_sku and (tem_vendas60 or tem_estoque_full_like or tem_transito_like):
        return "FULL"
    if tem_sku and tem_estoque_generico:
        return "FISICO"
    if tem_sku and tem_qtd_livre and not tem_preco:
        return "VENDAS"
    return "DESCONHECIDO"

def mapear_colunas(df: pd.DataFrame, tipo: str) -> pd.DataFrame:
    if tipo == "FULL":
        if "sku" in df.columns:           df["SKU"]=df["sku"].map(norm_sku)
        elif "codigo" in df.columns:      df["SKU"]=df["codigo"].map(norm_sku)
        elif "codigo_sku" in df.columns:  df["SKU"]=df["codigo_sku"].map(norm_sku)
        else: raise RuntimeError("FULL inválido: precisa de SKU/codigo.")

        c_v=[c for c in df.columns if c in ["vendas_qtd_60d","vendas_60d","vendas 60d"] or c.startswith("vendas_60d")]
        if not c_v: raise RuntimeError("FULL inválido: faltou Vendas_60d.")
        df["Vendas_Qtd_60d"]=df[c_v[0]].map(br_to_float).fillna(0).astype(int)

        c_e=[c for c in df.columns if c in ["estoque_full","estoque_atual"] or ("estoque" in c and "full" in c)]
        if not c_e: raise RuntimeError("FULL inválido: faltou Estoque_Full/estoque_atual.")
        df["Estoque_Full"]=df[c_e[0]].map(br_to_float).fillna(0).astype(int)

        c_t=[c for c in df.columns if c in ["em_transito","em transito","em_transito_full","em_transito_do_anuncio"] or ("transito" in c)]
        df["Em_Transito"]=df[c_t[0]].map(br_to_float).fillna(0).astype(int) if c_t else 0
        return df[["SKU","Vendas_Qtd_60d","Estoque_Full","Em_Transito"]].copy()

        if tipo == "FISICO":
        sku_series = (
            df.get("sku") or df.get("codigo") or df.get("codigo_sku") or
            df[df.columns[[i for i,c in enumerate(df.columns) if "sku" in c.lower()][0]]]  # primeira que contém 'sku'
            if any("sku" in c.lower() for c in df.columns) else None
        )
        if sku_series is None:
            raise RuntimeError("FÍSICO inválido: não achei coluna de SKU.")
        df["SKU"] = sku_series.map(norm_sku)

        # estoque (aceita várias)
        cand_estoque = [c for c in df.columns if c in ["estoque_atual","qtd","quantidade","estoque","saldo","qtde"] or "estoque" in c.lower()]
        if not cand_estoque:
            raise RuntimeError("FÍSICO inválido: faltou coluna de Estoque.")
        df["Estoque_Fisico"] = df[cand_estoque[0]].map(br_to_float).fillna(0).astype(int)

        # preço opcional — se não tiver, vira 0
        cand_preco = [c for c in df.columns if c.lower() in {"preco","preço","preco_compra","preco_medio","preco_unitario","custo","custo_medio"}]
        if cand_preco:
            df["Preco"] = df[cand_preco[0]].map(br_to_float).fillna(0.0)
        else:
            df["Preco"] = 0.0

        return df[["SKU","Estoque_Fisico","Preco"]].copy()

        if tipo == "VENDAS":
        sku_col = next((c for c in df.columns if "sku" in c.lower()), None)
        if sku_col is None:
            raise RuntimeError("VENDAS inválido: não achei coluna de SKU.")
        df["SKU"] = df[sku_col].map(norm_sku)

        sinais = [
            ("qtde", 4), ("quantidade", 4), ("quantity", 4), ("qtd", 4),
            ("vendas", 3), ("vendidos", 3), ("sold", 3),
            ("orders", 2), ("pedidos", 2), ("ordens", 2), ("unid", 2)
        ]
        melhor, melhor_score = None, -1
        for c in df.columns:
            cl = c.lower()
            score = sum(w for k, w in sinais if k in cl)
            if score > melhor_score:
                melhor, melhor_score = c, score

        if melhor is None or melhor_score <= 0:
            raise RuntimeError("VENDAS inválido: não achei coluna de Quantidade.")
        df["Quantidade"] = df[melhor].map(br_to_float).fillna(0).astype(int)
        return df[["SKU","Quantidade"]].copy()


# ======= Explosão & cálculo =======
def construir_kits_efetivo(cat: Catalogo) -> pd.DataFrame:
    kits = cat.kits_reais.copy()
    existentes = set(kits["kit_sku"].unique())
    alias=[]
    for s in cat.catalogo_simples["component_sku"].unique().tolist():
        s = norm_sku(s)
        if s and s not in existentes:
            alias.append((s,s,1))
    if alias:
        kits = pd.concat([kits, pd.DataFrame(alias, columns=["kit_sku","component_sku","qty"])], ignore_index=True)
    return kits.drop_duplicates(["kit_sku","component_sku"])

def explodir_por_kits(df: pd.DataFrame, kits: pd.DataFrame, sku_col: str, qtd_col: str) -> pd.DataFrame:
    base = df.copy()
    base["kit_sku"]=base[sku_col].map(norm_sku)
    base["qtd"]=base[qtd_col].astype(int)
    merged = base.merge(kits, on="kit_sku", how="left")
    exploded = merged.dropna(subset=["component_sku"]).copy()
    exploded["qty"]=exploded["qty"].astype(int)
    exploded["quantidade_comp"]=exploded["qtd"]*exploded["qty"]
    out = exploded.groupby("component_sku", as_index=False)["quantidade_comp"].sum()
    return out.rename(columns={"component_sku":"SKU","quantidade_comp":"Quantidade"})

def calcular(full_df, fisico_df, vendas_df, cat: Catalogo, h=60, g=0.0, LT=0):
    kits = construir_kits_efetivo(cat)
    full = full_df.copy()
    full["SKU"]=full["SKU"].map(norm_sku)
    full["Vendas_Qtd_60d"]=full["Vendas_Qtd_60d"].astype(int)
    full["Estoque_Full"]=full["Estoque_Full"].astype(int)
    full["Em_Transito"]=full["Em_Transito"].astype(int)

    shp = vendas_df.copy()
    shp["SKU"]=shp["SKU"].map(norm_sku)
    shp["Quantidade_60d"]=shp["Quantidade"].astype(int)

    ml_comp = explodir_por_kits(full[["SKU","Vendas_Qtd_60d"]].rename(columns={"SKU":"kit_sku","Vendas_Qtd_60d":"Qtd"}), kits,"kit_sku","Qtd").rename(columns={"Quantidade":"ML_60d"})
    shopee_comp = explodir_por_kits(shp[["SKU","Quantidade_60d"]].rename(columns={"SKU":"kit_sku","Quantidade_60d":"Qtd"}), kits,"kit_sku","Qtd").rename(columns={"Quantidade":"Shopee_60d"})

    cat_df = cat.catalogo_simples[["component_sku","fornecedor","status_reposicao"]].rename(columns={"component_sku":"SKU"})
    demanda = cat_df.merge(ml_comp, on="SKU", how="left").merge(shopee_comp, on="SKU", how="left")
    demanda[["ML_60d","Shopee_60d"]]=demanda[["ML_60d","Shopee_60d"]].fillna(0).astype(int)
    demanda["TOTAL_60d"]=np.maximum(demanda["ML_60d"]+demanda["Shopee_60d"], demanda["ML_60d"]).astype(int)

    fis = fisico_df.copy()
    fis["SKU"]=fis["SKU"].map(norm_sku)
    fis["Estoque_Fisico"]=fis["Estoque_Fisico"].fillna(0).astype(int)
    fis["Preco"]=fis["Preco"].fillna(0.0)

    base = demanda.merge(fis, on="SKU", how="left")
    base["Estoque_Fisico"]=base["Estoque_Fisico"].fillna(0).astype(int)
    base["Preco"]=base["Preco"].fillna(0.0)

    fator=(1.0+g/100.0)**(h/30.0)
    fk=full.copy()
    fk["vendas_dia"]=fk["Vendas_Qtd_60d"]/60.0
    fk["alvo"]=np.round(fk["vendas_dia"]*(LT+h)*fator).astype(int)
    fk["oferta"]=(fk["Estoque_Full"]+fk["Em_Transito"]).astype(int)
    fk["envio_desejado"]=(fk["alvo"]-fk["oferta"]).clip(lower=0).astype(int)

    necessidade = explodir_por_kits(fk[["SKU","envio_desejado"]].rename(columns={"SKU":"kit_sku","envio_desejado":"Qtd"}), kits,"kit_sku","Qtd").rename(columns={"Quantidade":"Necessidade"})
    base = base.merge(necessidade, on="SKU", how="left")
    base["Necessidade"]=base["Necessidade"].fillna(0).astype(int)

    base["Demanda_dia"]=base["TOTAL_60d"]/60.0
    base["Reserva_30d"]=np.round(base["Demanda_dia"]*30).astype(int)
    base["Folga_Fisico"]=(base["Estoque_Fisico"]-base["Reserva_30d"]).clip(lower=0).astype(int)
    base["Compra_Sugerida"]=(base["Necessidade"]-base["Folga_Fisico"]).clip(lower=0).astype(int)

    mask_nao=base["status_reposicao"].str.lower().str.contains("nao_repor", na=False)
    base.loc[mask_nao,"Compra_Sugerida"]=0

    base["Valor_Compra_R$"]=(base["Compra_Sugerida"].astype(float)*base["Preco"].astype(float)).round(2)
    base["Vendas_h_ML"]=np.round(base["ML_60d"]*(h/60.0)).astype(int)
    base["Vendas_h_Shopee"]=np.round(base["Shopee_60d"]*(h/60.0)).astype(int)

    base=base.sort_values(["fornecedor","Valor_Compra_R$","SKU"], ascending=[True,False,True])
    df_final = base[["SKU","fornecedor","Vendas_h_ML","Vendas_h_Shopee","Estoque_Fisico","Preco","Compra_Sugerida","Valor_Compra_R$","ML_60d","Shopee_60d","TOTAL_60d","Reserva_30d","Folga_Fisico","Necessidade"]].reset_index(drop=True)

    fis_unid=int(fis["Estoque_Fisico"].sum())
    fis_valor=float((fis["Estoque_Fisico"]*fis["Preco"]).sum())

    full_stock_comp = explodir_por_kits(full[["SKU","Estoque_Full"]].rename(columns={"SKU":"kit_sku","Estoque_Full":"Qtd"}), kits,"kit_sku","Qtd")
    full_stock_comp = full_stock_comp.merge(fis[["SKU","Preco"]], on="SKU", how="left")
    full_unid=int(full["Estoque_Full"].sum())
    full_valor=float((full_stock_comp["Quantidade"].fillna(0)*full_stock_comp["Preco"].fillna(0.0)).sum())

    painel={"full_unid":full_unid,"full_valor":full_valor,"fisico_unid":fis_unid,"fisico_valor":fis_valor}
    return df_final, painel

# ======= Export XLSX =======
def sha256_of_csv(df: pd.DataFrame) -> str:
    return hashlib.sha256(df.to_csv(index=False).encode("utf-8")).hexdigest()

def exportar_xlsx(df_final: pd.DataFrame, h: int, params: dict) -> bytes:
    int_cols=["Vendas_h_ML","Vendas_h_Shopee","Estoque_Fisico","Compra_Sugerida","Reserva_30d","Folga_Fisico","Necessidade","ML_60d","Shopee_60d","TOTAL_60d"]
    for c in int_cols:
        bad = df_final.index[(df_final[c] < 0) | (df_final[c].astype(float)%1!=0)]
        if len(bad)>0:
            linha=int(bad[0])+2; sku=df_final.loc[bad[0],"SKU"] if "SKU" in df_final.columns else "?"
            raise RuntimeError(f"Auditoria: '{c}' precisa ser inteiro ≥ 0 (ex.: linha {linha}, SKU={sku}).")
    calc=(df_final["Compra_Sugerida"]*df_final["Preco"]).round(2).values
    if not np.allclose(df_final["Valor_Compra_R$"].values, calc):
        raise RuntimeError("Auditoria: 'Valor_Compra_R$' ≠ 'Compra_Sugerida × Preco'.")

    output=io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        lista=df_final[df_final["Compra_Sugerida"]>0].copy()
        lista.to_excel(writer, sheet_name="Lista_Final", index=False)
        ws=writer.sheets["Lista_Final"]
        for i,col in enumerate(lista.columns):
            width=max(12, int(lista[col].astype(str).map(len).max())+2)
            ws.set_column(i,i,min(width,40))
        ws.freeze_panes(1,0); ws.autofilter(0,0,len(lista), len(lista.columns)-1)
        ctrl=pd.DataFrame([{
            "data_hora": dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "h":h, "hash_sha256": sha256_of_csv(df_final)
        } | params])
        ctrl.to_excel(writer, sheet_name="Controle", index=False)
    output.seek(0); return output.read()

# =========================================
# SIDEBAR — Padrão (KITS/CAT)
# =========================================
with st.sidebar:
    st.subheader("Parâmetros")
    h  = st.selectbox("Horizonte (dias)", [30,60,90], index=1)
    g  = st.number_input("Crescimento % ao mês", value=0.0, step=1.0)
    LT = st.number_input("Lead time (dias)", value=0, step=1, min_value=0)

    st.markdown("---")
    st.subheader("Padrão (KITS/CAT) — Google Sheets")
    st.caption("Carrega **somente** ao clicar. Se der erro, use o link alternativo.")
    colA, colB = st.columns(2)
    with colA:
        if st.button("Carregar padrão agora", use_container_width=True):
            try:
                cat = carregar_padrao_do_xlsx(DEFAULT_SHEET_ID)
                st.session_state.catalogo_df = cat.catalogo_simples.rename(columns={"component_sku":"sku"})
                st.session_state.kits_df = cat.kits_reais
                st.session_state.loaded_at = dt.datetime.now().strftime("%Y-%m-%d %H:%M")
                st.success("Padrão carregado.")
            except Exception as e:
                st.error(str(e))
    with colB:
        st.link_button("🔗 Abrir no Drive", DEFAULT_SHEET_LINK, use_container_width=True)

    st.text_input("Link alternativo (opcional)", key="alt_sheet_link")
    if st.button("Carregar deste link", use_container_width=True):
        try:
            cat = carregar_padrao_do_link(st.session_state.alt_sheet_link.strip())
            st.session_state.catalogo_df = cat.catalogo_simples.rename(columns={"component_sku":"sku"})
            st.session_state.kits_df = cat.kits_reais
            st.session_state.loaded_at = dt.datetime.now().strftime("%Y-%m-%d %H:%M")
            st.success("Padrão carregado do link.")
        except Exception as e:
            st.error(str(e))

# =========================================
# ABAS
# =========================================
tab_dados, tab_compra, tab_aloc = st.tabs(["📂 Dados das Empresas","🧮 Compra Automática","📦 Alocação de Compra"])

# =========================================
# 📂 1) DADOS DAS EMPRESAS — Uploads fixos com persistência
# =========================================
with tab_dados:
    st.subheader("Uploads fixos por empresa (mantidos até você limpar)")
    st.caption("FULL, Shopee/MT e ESTOQUE ficam gravados no servidor do app até você clicar em 'Limpar' (ou até um redeploy).")

    def bloco_empresa(nome: str):
        st.markdown(f"### {nome}")
        c1, c2 = st.columns(2)
        with c1:
            st.markdown(f"**FULL — {nome}**")
            up_full = st.file_uploader("CSV/XLSX/XLS", type=["csv","xlsx","xls"], key=f"{nome}_FULL")
        with c2:
            st.markdown(f"**Shopee/MT — {nome}**")
            up_v = st.file_uploader("CSV/XLSX/XLS", type=["csv","xlsx","xls"], key=f"{nome}_VENDAS")

        st.markdown("**Estoque Físico — opcional (necessário só para Compra Automática)**")
        up_e = st.file_uploader("CSV/XLSX/XLS", type=["csv","xlsx","xls"], key=f"{nome}_ESTOQUE")

        colS, colL = st.columns([1,1])
        with colS:
            if st.button(f"Salvar {nome}", use_container_width=True, key=f"save_{nome}"):
                try:
                    if up_full:  save_upload(nome, "FULL", up_full)
                    if up_v:     save_upload(nome, "VENDAS", up_v)
                    if up_e:     save_upload(nome, "ESTOQUE", up_e)
                    st.success(f"Arquivos de {nome} salvos.")
                except Exception as e:
                    st.error(str(e))
        with colL:
            if st.button(f"Limpar {nome}", use_container_width=True, key=f"clear_{nome}"):
                clear_company(nome); st.warning(f"{nome} limpo.")

        meta = load_meta(nome)
        def _fmt(kind):
            ent = load_entry(nome, kind)
            if not ent: return "—"
            return f"{ent['name']} ({ent['ts']})"
        st.caption(f"Status {nome}: FULL [{_fmt('FULL')}] • Shopee [{_fmt('VENDAS')}] • Estoque [{_fmt('ESTOQUE')}]")
        st.markdown("---")

    bloco_empresa("ALIVVIA")
    bloco_empresa("JCA")

# =========================================
# 🧮 2) COMPRA AUTOMÁTICA — usa apenas arquivos salvos
# =========================================
with tab_compra:
    st.subheader("Gerar Compra (por empresa) — lógica original")
    empresa = st.radio("Empresa ativa", ["ALIVVIA","JCA"], horizontal=True, key="empresa_compra")

    # Mostra arquivos em uso
    def _disp(ent):
        return f"{ent['name']} ({ent['ts']})" if ent else "—"
    f_ent = load_entry(empresa,"FULL")
    v_ent = load_entry(empresa,"VENDAS")
    e_ent = load_entry(empresa,"ESTOQUE")
    c1,c2,c3 = st.columns(3)
    with c1: st.info(f"FULL: {_disp(f_ent)}")
    with c2: st.info(f"Shopee/MT: {_disp(v_ent)}")
    with c3: st.info(f"Estoque: {_disp(e_ent)}")
    st.caption("Para atualizar, vá em **Dados das Empresas**.")

    st.markdown("---")
       # Substitui o bloco de detecção/mapeamento por mensagens mais claras
    dfs = []
    for nome, df_up in [("FULL", full_raw), ("FÍSICO", fisico_raw), ("VENDAS (Shopee/MT)", vendas_raw)]:
        t = mapear_tipo(df_up)
        if t == "DESCONHECIDO":
            # Mostra quais cabeçalhos chegaram para facilitar o ajuste
            st.error(
                f"{empresa}: arquivo **{nome}** não foi reconhecido.\n\n"
                f"**Cabeçalhos lidos**: {list(df_up.columns)[:12]}"
            )
            st.stop()
        try:
            df_mapeado = mapear_colunas(df_up, t)
        except Exception as e:
            st.error(f"{empresa}: falha ao interpretar **{nome}** ({t}). Detalhe: {e}")
            st.stop()
        dfs.append((t, df_mapeado))

            full_df   = [df for t,df in dfs if t=="FULL"][0]
            fisico_df = [df for t,df in dfs if t=="FISICO"][0]
            vendas_df = [df for t,df in dfs if t=="VENDAS"][0]

            cat = Catalogo(
                catalogo_simples=st.session_state.catalogo_df.rename(columns={"sku":"component_sku"}),
                kits_reais=st.session_state.kits_df
            )
            df_final, painel = calcular(full_df, fisico_df, vendas_df, cat, h=h, g=g, LT=LT)
            st.session_state.df_final = df_final
            st.session_state.painel   = painel
            st.success("Cálculo concluído. Role para visualizar e exportar.")
        except Exception as e:
            st.error(f"Falha ao calcular: {e}")

    # Pós-cálculo (painel + export)
    if st.session_state.get("df_final") is not None:
        df_final = st.session_state.df_final.copy()
        painel   = st.session_state.painel
        st.subheader("📊 Painel")
        a,b,c,d = st.columns(4)
        a.metric("Full (un)", f"{painel['full_unid']:,}".replace(",","."))
        b.metric("Full (R$)", f"R$ {painel['full_valor']:,.2f}")
        c.metric("Físico (un)", f"{painel['fisico_unid']:,}".replace(",","."))
        d.metric("Físico (R$)", f"R$ {painel['fisico_valor']:,.2f}")

        st.subheader("Itens para comprar")
        st.dataframe(
            df_final[df_final["Compra_Sugerida"]>0][["SKU","fornecedor","Vendas_h_ML","Vendas_h_Shopee","Estoque_Fisico","Preco","Compra_Sugerida","Valor_Compra_R$"]],
            use_container_width=True
        )
        if st.checkbox("Gerar XLSX (sem recálculo)"):
            try:
                x = exportar_xlsx(df_final, h=h, params={"g":g,"LT":LT,"empresa":empresa})
                st.download_button("Baixar XLSX", data=x, file_name=f"Compra_Sugerida_{h}d_{empresa}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            except Exception as e:
                st.error(f"Exportação bloqueada: {e}")

# =========================================
# 📦 3) ALOCAÇÃO DE COMPRA — proporcional às vendas (FULL + Shopee)
# =========================================
with tab_aloc:
    st.subheader("Distribuir quantidade entre empresas — proporcional às vendas (FULL + Shopee)")
    st.caption("Não usa estoque. Usa vendas de FULL (60d) + Shopee/MT, explodidas por KITS para o componente.")

    # Catálogo para autocomplete
    if st.session_state.catalogo_df is None or st.session_state.kits_df is None:
        st.warning("Carregue o **Padrão (KITS/CAT)** no sidebar para habilitar a alocação.")
        st.stop()

    cat_df = st.session_state.catalogo_df
    skus_comp = cat_df["sku"].dropna().astype(str).sort_values().unique().tolist()
    sku_comp = st.selectbox("SKU do componente para alocar", skus_comp)
    qtd_lote = st.number_input("Quantidade total do lote (ex.: 400)", min_value=1, value=1000, step=10)

    if st.button("Calcular alocação proporcional", type="primary"):
        try:
            kits = construir_kits_efetivo(Catalogo(
                catalogo_simples=cat_df.rename(columns={"sku":"component_sku"}),
                kits_reais=st.session_state.kits_df
            ))

            # Coleta vendas 60d explodidas de cada empresa (FULL + Shopee)
            def demanda_empresa(emp: str) -> int:
                f_ent = load_entry(emp,"FULL"); v_ent = load_entry(emp,"VENDAS")
                if not f_ent or not v_ent: return 0
                full_raw = mapear_colunas(read_df_from_entry(f_ent), mapear_tipo(read_df_from_entry(f_ent)))
                vendas_raw = mapear_colunas(read_df_from_entry(v_ent), mapear_tipo(read_df_from_entry(v_ent)))

                ml_comp = explodir_por_kits(full_raw[["SKU","Vendas_Qtd_60d"]].rename(columns={"SKU":"kit_sku","Vendas_Qtd_60d":"Qtd"}), kits,"kit_sku","Qtd")
                shp_comp = explodir_por_kits(vendas_raw[["SKU","Quantidade"]].rename(columns={"SKU":"kit_sku","Quantidade":"Qtd"}), kits,"kit_sku","Qtd")
                # soma e pega somente o SKU componente desejado
                total = 0
                if not ml_comp.empty:
                    total += int(ml_comp.loc[ml_comp["SKU"]==sku_comp, "Quantidade"].sum())
                if not shp_comp.empty:
                    total += int(shp_comp.loc[shp_comp["SKU"]==sku_comp, "Quantidade"].sum())
                return total

            d_alivvia = demanda_empresa("ALIVVIA")
            d_jca     = demanda_empresa("JCA")
            soma = d_alivvia + d_jca

            rows=[]
            if soma == 0:
                # sem base → 50/50
                pA=pJ=0.5
                rows=[["ALIVVIA", sku_comp, d_alivvia, pA, int(round(qtd_lote*pA))],
                      ["JCA",     sku_comp, d_jca,     pJ, int(round(qtd_lote*pJ))]]
                st.warning("Sem vendas detectadas para esse componente — alocação 50/50 por falta de base.")
            else:
                pA = d_alivvia/soma
                pJ = d_jca/soma
                rows=[["ALIVVIA", sku_comp, d_alivvia, pA, int(round(qtd_lote*pA))],
                      ["JCA",     sku_comp, d_jca,     pJ, int(round(qtd_lote*pJ))]]

            df_aloc = pd.DataFrame(rows, columns=["Empresa","SKU","Demanda_60d","Proporção","Alocação_Sugerida"])
            st.dataframe(df_aloc, use_container_width=True)

            st.success(f"Total alocado: {df_aloc['Alocação_Sugerida'].sum()} un (ALIVVIA {df_aloc.loc[df_aloc['Empresa']=='ALIVVIA','Alocação_Sugerida'].sum()} | JCA {df_aloc.loc[df_aloc['Empresa']=='JCA','Alocação_Sugerida'].sum()})")
            csv = df_aloc.to_csv(index=False).encode("utf-8")
            st.download_button("Baixar alocação (.csv)", data=csv, file_name=f"alocacao_{sku_comp}_{dt.datetime.now().strftime('%Y%m%d_%H%M')}.csv", mime="text/csv")
        except Exception as e:
            st.error(f"Falha ao calcular alocação: {e}")

st.caption("© Alivvia — simples, robusto e auditável.")
