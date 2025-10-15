# Reposição Logística — Alivvia (Streamlit)
# Multi-empresa (ALIVVIA / JCA) + Plano do Mês + Compras Manuais + Saldo
# Sem alterar regras de cálculo originais.

import os
import io
import json
import hashlib
import datetime as dt
from dataclasses import dataclass
from typing import Dict, List, Any

import numpy as np
import pandas as pd
from unidecode import unidecode
import streamlit as st
import requests
import base64
from urllib.parse import urlparse

# ======== URLs padrão (GOOGLE DRIVE) ========
PADRAO_URL_DEFAULT = "https://docs.google.com/spreadsheets/d/1cTLARjq-B5g50dL6tcntg7lb_Iu0ta43/export?format=xlsx"
PADRAO_URL_EDIT    = "https://docs.google.com/spreadsheets/d/1cTLARjq-B5g50dL6tcntg7lb_Iu0ta43/edit"

# =============== Utils ===============
def br_to_float(x):
    if pd.isna(x):
        return np.nan
    if isinstance(x, (int, float, np.integer, np.floating)):
        return float(x)
    s = str(x).strip()
    if s == "":
        return np.nan
    s = s.replace("\u00a0", " ")
    s = s.replace("R$", "").replace(" ", "")
    s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except Exception:
        return np.nan

def norm_header(s: str) -> str:
    s = (s or "").strip().lower()
    s = unidecode(s)
    for ch in [" ", "-", "(", ")", "/", "\\", "[", "]", "."]:
        s = s.replace(ch, "_")
    while "__" in s:
        s = s.replace("__", "_")
    return s.strip("_")

def norm_sku(x: str) -> str:
    if pd.isna(x):
        return ""
    return unidecode(str(x)).strip().upper()

def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

def sha256_of_csv(df: pd.DataFrame) -> str:
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    return hashlib.sha256(csv_bytes).hexdigest()

@dataclass
class Catalogo:
    catalogo_simples: pd.DataFrame
    kits_reais: pd.DataFrame

# =============== Padrão produtos (KITS + CAT) ===============
def carregar_padrao_produtos(caminho: str) -> Catalogo:
    try:
        xls = pd.ExcelFile(caminho)
    except Exception as e:
        raise RuntimeError(f"Não consegui abrir '{caminho}'. Deixe na mesma pasta do app. Erro: {e}")

    def load_sheet(opts):
        for n in opts:
            if n in xls.sheet_names:
                return pd.read_excel(xls, n)
        raise RuntimeError(f"Aba não encontrada. Esperado uma de {opts} no '{caminho}'.")

    df_kits = load_sheet(["KITS", "KITS_REAIS", "kits", "kits_reais"]).copy()
    df_cat  = load_sheet(["CATALOGO_SIMPLES", "CATALOGO", "catalogo_simples", "catalogo"]).copy()

    df_kits.columns = [norm_header(c) for c in df_kits.columns]
    if "qty" not in df_kits.columns and "qty_por_kit" not in df_kits.columns:
        raise RuntimeError("Na KITS: precisa de 'kit_sku', 'component_sku', 'qty(>=1)'.")
    if "qty" not in df_kits.columns:
        df_kits["qty"] = df_kits.get("qty_por_kit", 1)

    for c in ["kit_sku", "component_sku", "qty"]:
        if c not in df_kits.columns:
            raise RuntimeError("Na KITS: faltam colunas obrigatórias.")

    df_kits = df_kits[["kit_sku", "component_sku", "qty"]].copy()
    df_kits["kit_sku"]       = df_kits["kit_sku"].map(norm_sku)
    df_kits["component_sku"] = df_kits["component_sku"].map(norm_sku)
    df_kits["qty"]           = df_kits["qty"].map(br_to_float).fillna(0).astype(int)
    df_kits = df_kits[df_kits["qty"] >= 1]
    df_kits = df_kits.drop_duplicates(subset=["kit_sku", "component_sku"], keep="first")

    df_cat.columns = [norm_header(c) for c in df_cat.columns]
    if "component_sku" not in df_cat.columns:
        raise RuntimeError("CATALOGO_SIMPLES precisa ter a coluna 'component_sku'.")
    if "fornecedor" not in df_cat.columns:
        df_cat["fornecedor"] = ""
    if "status_reposicao" not in df_cat.columns:
        df_cat["status_reposicao"] = ""

    df_cat["component_sku"]   = df_cat["component_sku"].map(norm_sku)
    df_cat["fornecedor"]      = df_cat["fornecedor"].fillna("").astype(str)
    df_cat["status_reposicao"]= df_cat["status_reposicao"].fillna("").astype(str)

    return Catalogo(catalogo_simples=df_cat, kits_reais=df_kits)

def construir_kits_efetivo(cat: Catalogo) -> pd.DataFrame:
    kits = cat.kits_reais.copy()
    existentes = set(kits["kit_sku"].unique())
    alias = []
    for s in cat.catalogo_simples["component_sku"].unique().tolist():
        s = norm_sku(s)
        if s and s not in existentes:
            alias.append((s, s, 1))
    if alias:
        kits = pd.concat([kits, pd.DataFrame(alias, columns=["kit_sku","component_sku","qty"])], ignore_index=True)
    kits = kits.drop_duplicates(subset=["kit_sku", "component_sku"], keep="first")
    return kits

# =============== Leitura genérica ===============
def load_any_table(uploaded_file) -> pd.DataFrame:
    if uploaded_file is None:
        return None
    name = uploaded_file.name.lower()
    try:
        if name.endswith(".csv"):
            uploaded_file.seek(0)
            df = pd.read_csv(uploaded_file, dtype=str, keep_default_na=False, sep=None, engine="python")
        else:
            uploaded_file.seek(0)
            df = pd.read_excel(uploaded_file, dtype=str, keep_default_na=False)
    except Exception as e:
        raise RuntimeError(f"Não consegui ler o arquivo '{uploaded_file.name}': {e}")

    df.columns = [norm_header(c) for c in df.columns]

    # FULL com header na 3ª linha
    if ("sku" not in df.columns) and ("codigo" not in df.columns) and ("codigo_sku" not in df.columns) and len(df) > 0:
        try:
            uploaded_file.seek(0)
            df = pd.read_excel(uploaded_file, dtype=str, keep_default_na=False, header=2)
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

# =============== Detecção por conteúdo (tolerante) ===============
def mapear_tipo(df: pd.DataFrame) -> str:
    cols = [c.lower() for c in df.columns]
    tem_vendas60 = any(c.startswith("vendas_60d") or c in {"vendas 60d","vendas_qtd_60d"} for c in cols)
    tem_estoque  = any(c in {"estoque_full","estoque_atual"} for c in cols)
    tem_transito = any(c in {"em_transito","em transito","em_transito_full","em_transito_do_anuncio"} for c in cols)
    tem_sku_std  = any(c in {"sku","codigo","codigo_sku"} for c in cols)
    if tem_sku_std and ((tem_vendas60 and tem_estoque) or (tem_vendas60 and tem_transito) or (tem_estoque and tem_transito)):
        return "FULL"

    tem_preco = any(c in {"preco","preco_compra","preco_medio","custo","custo_medio"} for c in cols)
    tem_estoque_fis = any(c in {"estoque_atual","qtd","quantidade"} for c in cols)
    tem_sku_livre = any("sku" in c for c in cols)
    if tem_sku_livre and tem_estoque_fis and tem_preco:
        return "FISICO"

    tem_qtd_livre = any(("qtde" in c) or ("quant" in c) or ("venda" in c) or ("order" in c) for c in cols)
    if tem_sku_livre and tem_qtd_livre and not tem_preco:
        return "VENDAS"

    return "DESCONHECIDO"

# =============== Mapeamento de colunas ===============
def mapear_colunas(df: pd.DataFrame, tipo: str) -> pd.DataFrame:
    if tipo == "FULL":
        if "sku" in df.columns:
            df["SKU"] = df["sku"].map(norm_sku)
        elif "codigo" in df.columns:
            df["SKU"] = df["codigo"].map(norm_sku)
        elif "codigo_sku" in df.columns:
            df["SKU"] = df["codigo_sku"].map(norm_sku)
        else:
            raise RuntimeError("FULL inválido: precisa de coluna SKU/codigo.")

        c_v = [c for c in df.columns if c in ["vendas_qtd_60d","vendas_60d","vendas 60d"] or c.startswith("vendas_60d")]
        if not c_v: raise RuntimeError("FULL inválido: faltou Vendas_60d.")
        df["Vendas_Qtd_60d"] = df[c_v[0]].map(br_to_float).fillna(0).astype(int)

        c_e = [c for c in df.columns if c in ["estoque_full","estoque_atual"]]
        if not c_e: raise RuntimeError("FULL inválido: faltou Estoque_Full/estoque_atual.")
        df["Estoque_Full"] = df[c_e[0]].map(br_to_float).fillna(0).astype(int)

        c_t = [c for c in df.columns if c in ["em_transito","em transito","em_transito_full","em_transito_do_anuncio"]]
        df["Em_Transito"] = df[c_t[0]].map(br_to_float).fillna(0).astype(int) if c_t else 0

        return df[["SKU","Vendas_Qtd_60d","Estoque_Full","Em_Transito"]].copy()

    if tipo == "FISICO":
        sku_series = (
            df["sku"] if "sku" in df.columns else
            (df["codigo"] if "codigo" in df.columns else
             (df["codigo_sku"] if "codigo_sku" in df.columns else None))
        )
        if sku_series is None:
            sku_series = df[next(c for c in df.columns if "sku" in c.lower())]
        df["SKU"] = sku_series.map(norm_sku)

        c_q = [c for c in df.columns if c in ["estoque_atual","qtd","quantidade"]]
        if not c_q: raise RuntimeError("FÍSICO inválido: faltou Estoque (estoque_atual/qtd/quantidade).")
        df["Estoque_Fisico"] = df[c_q[0]].map(br_to_float).fillna(0).astype(int)

        c_p = [c for c in df.columns if c in ["preco","preco_compra","custo","custo_medio","preco_medio"]]
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
            cl = c.lower()
            score = 0
            if "qtde"  in cl: score += 3
            if "quant" in cl: score += 2
            if "venda" in cl: score += 1
            if "order" in cl: score += 1
            if score > 0:
                cand_qty.append((score, c))
        if not cand_qty:
            raise RuntimeError("VENDAS inválido: não achei coluna de Quantidade.")
        cand_qty.sort(reverse=True)
        qcol = cand_qty[0][1]

        df["Quantidade"] = df[qcol].map(br_to_float).fillna(0).astype(int)
        return df[["SKU","Quantidade"]].copy()

    raise RuntimeError("Tipo de arquivo desconhecido.")

# =============== Explosão e Cálculo (inalterado) ===============
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

def calcular(full_df, fisico_df, vendas_df, cat: Catalogo, h=60, g=0.0, LT=0):
    kits = construir_kits_efetivo(cat)

    full = full_df.copy()
    full["SKU"]             = full["SKU"].map(norm_sku)
    full["Vendas_Qtd_60d"]  = full["Vendas_Qtd_60d"].astype(int)
    full["Estoque_Full"]    = full["Estoque_Full"].astype(int)
    full["Em_Transito"]     = full["Em_Transito"].astype(int)

    shp = vendas_df.copy()
    shp["SKU"]            = shp["SKU"].map(norm_sku)
    shp["Quantidade_60d"] = shp["Quantidade"].astype(int)

    ml_comp = explodir_por_kits(
        full[["SKU","Vendas_Qtd_60d"]].rename(columns={"SKU":"kit_sku","Vendas_Qtd_60d":"Qtd"}),
        kits,"kit_sku","Qtd"
    ).rename(columns={"Quantidade":"ML_60d"})

    shopee_comp = explodir_por_kits(
        shp[["SKU","Quantidade_60d"]].rename(columns={"SKU":"kit_sku","Quantidade_60d":"Qtd"}),
        kits,"kit_sku","Qtd"
    ).rename(columns={"Quantidade":"Shopee_60d"})

    cat_df = cat.catalogo_simples[["component_sku","fornecedor","status_reposicao"]].rename(columns={"component_sku":"SKU"})

    demanda = cat_df.merge(ml_comp, on="SKU", how="left").merge(shopee_comp, on="SKU", how="left")
    demanda[["ML_60d","Shopee_60d"]] = demanda[["ML_60d","Shopee_60d"]].fillna(0).astype(int)
    demanda["TOTAL_60d"] = np.maximum(demanda["ML_60d"] + demanda["Shopee_60d"], demanda["ML_60d"]).astype(int)

    fis = fisico_df.copy()
    fis["SKU"]            = fis["SKU"].map(norm_sku)
    fis["Estoque_Fisico"] = fis["Estoque_Fisico"].fillna(0).astype(int)
    fis["Preco"]          = fis["Preco"].fillna(0.0)

    base = demanda.merge(fis, on="SKU", how="left")
    base["Estoque_Fisico"] = base["Estoque_Fisico"].fillna(0).astype(int)
    base["Preco"]          = base["Preco"].fillna(0.0)

    fator = (1.0 + g/100.0) ** (h/30.0)
    fk = full.copy()
    fk["vendas_dia"]     = fk["Vendas_Qtd_60d"] / 60.0
    fk["alvo"]           = np.round(fk["vendas_dia"] * (LT + h) * fator).astype(int)
    fk["oferta"]         = (fk["Estoque_Full"] + fk["Em_Transito"]).astype(int)
    fk["envio_desejado"] = (fk["alvo"] - fk["oferta"]).clip(lower=0).astype(int)

    necessidade = explodir_por_kits(
        fk[["SKU","envio_desejado"]].rename(columns={"SKU":"kit_sku","envio_desejado":"Qtd"}),
        kits,"kit_sku","Qtd"
    ).rename(columns={"Quantidade":"Necessidade"})

    base = base.merge(necessidade, on="SKU", how="left")
    base["Necessidade"] = base["Necessidade"].fillna(0).astype(int)

    base["Demanda_dia"]  = base["TOTAL_60d"] / 60.0
    base["Reserva_30d"]  = np.round(base["Demanda_dia"] * 30).astype(int)
    base["Folga_Fisico"] = (base["Estoque_Fisico"] - base["Reserva_30d"]).clip(lower=0).astype(int)

    base["Compra_Sugerida"] = (base["Necessidade"] - base["Folga_Fisico"]).clip(lower=0).astype(int)

    mask_nao = base["status_reposicao"].str.lower().str.contains("nao_repor", na=False)
    base.loc[mask_nao, "Compra_Sugerida"] = 0

    base["Valor_Compra_R$"] = (base["Compra_Sugerida"].astype(float) * base["Preco"].astype(float)).round(2)

    base["Vendas_h_ML"]     = np.round(base["ML_60d"] * (h/60.0)).astype(int)
    base["Vendas_h_Shopee"] = np.round(base["Shopee_60d"] * (h/60.0)).astype(int)

    base = base.sort_values(["fornecedor","Valor_Compra_R$","SKU"], ascending=[True, False, True])

    df_final = base[[
        "SKU","fornecedor",
        "Vendas_h_ML","Vendas_h_Shopee",
        "Estoque_Fisico","Preco","Compra_Sugerida","Valor_Compra_R$",
        "ML_60d","Shopee_60d","TOTAL_60d","Reserva_30d","Folga_Fisico","Necessidade"
    ]].reset_index(drop=True)

    fis_unid  = int(fis["Estoque_Fisico"].sum())
    fis_valor = float((fis["Estoque_Fisico"] * fis["Preco"]).sum())

    full_stock_comp = explodir_por_kits(
        full[["SKU","Estoque_Full"]].rename(columns={"SKU":"kit_sku","Estoque_Full":"Qtd"}),
        kits,"kit_sku","Qtd"
    )
    full_stock_comp = full_stock_comp.merge(fis[["SKU","Preco"]], on="SKU", how="left")
    full_unid  = int(full["Estoque_Full"].sum())
    full_valor = float((full_stock_comp["Quantidade"].fillna(0) * full_stock_comp["Preco"].fillna(0.0)).sum())

    painel = {"full_unid": full_unid, "full_valor": full_valor, "fisico_unid": fis_unid, "fisico_valor": fis_valor}
    return df_final, painel

# =============== Download de padrão via link ===============
def _normalize_onedrive_url(url: str) -> str:
    try:
        u = urlparse(url)
        host = (u.netloc or "").lower()
        path = (u.path or "").lower()
        if "onedrive.live.com" in host and "download" in path:
            return url
        if "1drv.ms" in host or "onedrive.live.com" in host:
            enc = base64.urlsafe_b64encode(url.encode("utf-8")).decode("ascii").rstrip("=")
            return f"https://api.onedrive.com/v1.0/shares/u!{enc}/root/content"
    except Exception:
        pass
    return url

def baixar_padrao(url: str, destino: str) -> dict:
    if not url or not url.strip():
        raise RuntimeError("URL do arquivo padrão não informada.")
    use_url = _normalize_onedrive_url(url.strip())
    try:
        r = requests.get(use_url, timeout=30, allow_redirects=True)
        r.raise_for_status()
    except Exception as e:
        raise RuntimeError(f"Falha ao baixar o padrão: {e}")
    content = r.content
    try:
        pd.ExcelFile(io.BytesIO(content))
    except Exception as e:
        raise RuntimeError(f"Arquivo baixado não é um XLSX válido: {e}")
    with open(destino, "wb") as f:
        f.write(content)
    return {"bytes": len(content), "sha256": sha256_bytes(content), "from_url": url, "saved_to": destino}

# =============== Export XLSX (lista final) ===============
def exportar_xlsx_lista_final(df_final: pd.DataFrame, h: int, params: dict) -> bytes:
    for c in ["Vendas_h_ML","Vendas_h_Shopee","Estoque_Fisico","Compra_Sugerida",
              "Reserva_30d","Folga_Fisico","Necessidade","ML_60d","Shopee_60d","TOTAL_60d"]:
        if (df_final[c] < 0).any() or (df_final[c].astype(float) % 1 != 0).any():
            raise RuntimeError(f"Auditoria: campo {c} precisa ser inteiro e ≥ 0.")
    if not np.allclose(df_final["Valor_Compra_R$"].values,
                       (df_final["Compra_Sugerida"] * df_final["Preco"]).round(2).values):
        raise RuntimeError("Auditoria: Valor_Compra_R$ inconsistente com Compra × Preco.")

    hash_str = sha256_of_csv(df_final)
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        lista = df_final[df_final["Compra_Sugerida"] > 0].copy()
        lista.to_excel(writer, sheet_name="Lista_Final", index=False)
        ws = writer.sheets["Lista_Final"]
        for i, col in enumerate(lista.columns):
            width = max(12, int(lista[col].astype(str).map(len).max()) + 2)
            ws.set_column(i, i, min(width, 40))
        ws.freeze_panes(1, 0); ws.autofilter(0, 0, len(lista), len(lista.columns)-1)

        ctrl = pd.DataFrame([{
            "data_hora": dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "h": h,
            "linhas_Lista_Final": int((df_final["Compra_Sugerida"] > 0).sum()),
            "soma_Compra_Sugerida": int(df_final["Compra_Sugerida"].sum()),
            "soma_Valor_Compra_R$": float(df_final["Valor_Compra_R$"].sum()),
            "hash_sha256": hash_str,
        } | params])
        ctrl.to_excel(writer, sheet_name="Controle", index=False)
    output.seek(0)
    return output.read()

# =============== Export XLSX (Plano/Compras/Saldo por empresa) ===============
def exportar_plano_compras_saldo(snapshots: Dict[str, pd.DataFrame],
                                 compras: Dict[str, pd.DataFrame],
                                 saldos: Dict[str, pd.DataFrame]) -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        for empresa in ["ALIVVIA","JCA"]:
            if empresa in snapshots and snapshots[empresa] is not None:
                snapshots[empresa].to_excel(writer, sheet_name=f"{empresa}_Plano", index=False)
            if empresa in compras and compras[empresa] is not None:
                compras[empresa].to_excel(writer, sheet_name=f"{empresa}_Compras", index=False)
            if empresa in saldos and saldos[empresa] is not None:
                saldos[empresa].to_excel(writer, sheet_name=f"{empresa}_Saldo", index=False)
    output.seek(0)
    return output.read()

# =============== Sessão (persistência JSON) ===============
def salvar_sessao_json(state: dict) -> bytes:
    serial = {}
    for k, v in state.items():
        try:
            if isinstance(v, pd.DataFrame):
                serial[k] = {"__df__": True, "data": v.to_dict(orient="records")}
            else:
                serial[k] = v
        except Exception:
            pass
    b = json.dumps(serial, ensure_ascii=False).encode("utf-8")
    return b

def carregar_sessao_json(b: bytes) -> dict:
    raw = json.loads(b.decode("utf-8"))
    out = {}
    for k, v in raw.items():
        if isinstance(v, dict) and v.get("__df__") and "data" in v:
            out[k] = pd.DataFrame(v["data"])
        else:
            out[k] = v
    return out

# =============== UI ===============
st.set_page_config(page_title="Reposição Logística — Alivvia", layout="wide")
st.title("Reposição Logística — Alivvia")
st.caption("Multi-empresa (ALIVVIA/JCA). FULL por anúncio; compra por componente; Shopee explode antes; plano do mês; compras manuais; saldo por empresa.")

# ---- Estado inicial
def ensure_state():
    if "padrao_url" not in st.session_state:
        st.session_state.padrao_url = PADRAO_URL_DEFAULT
    if "padrao_meta" not in st.session_state:
        st.session_state.padrao_meta = None
    if "padrao_override_path" not in st.session_state:
        st.session_state.padrao_override_path = None
    for emp in ["ALIVVIA","JCA"]:
        key = f"{emp}_df_final"
        if key not in st.session_state: st.session_state[key] = None
        key = f"{emp}_painel"
        if key not in st.session_state: st.session_state[key] = None
        key = f"{emp}_snapshot"
        if key not in st.session_state: st.session_state[key] = None
        key = f"{emp}_compras"
        if key not in st.session_state: st.session_state[key] = pd.DataFrame(columns=["data_compra","empresa","fornecedor","SKU","quantidade","obs"])
        key = f"{emp}_semanais"
        if key not in st.session_state: st.session_state[key] = []  # lista de SKUs
        key = f"{emp}_limites"
        if key not in st.session_state: st.session_state[key] = {}  # SKU -> limite_dias override
ensure_state()

# exibe meta do padrão baixado, se houver
if st.session_state.get("padrao_meta"):
    m = st.session_state["padrao_meta"]
    st.caption(f"Padrão em uso: {os.path.basename(m['saved_to'])} • {m['bytes']:,} bytes • SHA256 {m['sha256'][:12]}…")

with st.sidebar:
    st.subheader("Parâmetros")
    h  = st.selectbox("Horizonte (dias)", [30, 60, 90], index=1)
    g  = st.number_input("Crescimento % ao mês", value=0.0, step=1.0)
    LT = st.number_input("Lead time (dias)", value=0, step=1, min_value=0)

    st.markdown("---")
    st.subheader("Janela de compra (data limite)")
    janela_dias = st.number_input("Comprar dentro de (dias)", value=30, step=1, min_value=1)

    st.markdown("---")
    st.subheader("Padrão (KITS/CAT) por link")
    padrao_url = st.text_input(
        "URL direta do Padrao_produtos.xlsx (Drive recomendado)",
        value=st.session_state.get("padrao_url", PADRAO_URL_DEFAULT)
    )
    if padrao_url != st.session_state.get("padrao_url"):
        st.session_state.padrao_url = padrao_url

    if st.button("Resetar para Drive (padrão)", use_container_width=True):
        st.session_state.padrao_url = PADRAO_URL_DEFAULT
        st.success("URL resetada para o Google Drive (xlsx export).")

    st.markdown(f"[🔗 Abrir no Google Drive para editar]({PADRAO_URL_EDIT})")

    if st.button("Baixar padrão", use_container_width=True):
        try:
            base_dir = os.path.dirname(os.path.abspath(__file__))
            override_path = os.path.join(base_dir, "Padrao_produtos_atualizado.xlsx")
            meta = baixar_padrao(st.session_state.get("padrao_url", ""), override_path)
            st.session_state.padrao_override_path = override_path
            st.session_state.padrao_meta = meta
            st.success(f"Baixado com sucesso! SHA256: {meta['sha256'][:12]}…")
        except Exception as e:
            st.error(str(e))

    st.markdown("---")
    st.subheader("Sessão")
    colA, colB = st.columns(2)
    with colA:
        if st.button("Salvar sessão (JSON)"):
            b = salvar_sessao_json(st.session_state)
            st.download_button("Baixar sessão.json", b, file_name="sessao_compras.json", mime="application/json", use_container_width=True)
    with colB:
        up = st.file_uploader("Carregar sessão (JSON)", type=["json"])
        if up is not None:
            try:
                loaded = carregar_sessao_json(up.read())
                st.session_state.update(loaded)
                st.success("Sessão carregada.")
            except Exception as e:
                st.error(f"Falha ao carregar sessão: {e}")

# ====== Abas por empresa ======
tab_a, tab_j = st.tabs(["🏢 ALIVVIA", "🏢 JCA"])

def bloco_empresa(EMPRESA: str, tab):
    with tab:
        st.subheader(f"{EMPRESA} — Entradas")
        col1, col2, col3 = st.columns(3)
        with col1: full_file   = st.file_uploader(f"{EMPRESA}: FULL (Magiic)", key=f"{EMPRESA}_FULL")
        with col2: fisico_file = st.file_uploader(f"{EMPRESA}: Estoque Físico (CSV/XLSX/XLS)", key=f"{EMPRESA}_FIS")
        with col3: vendas_file = st.file_uploader(f"{EMPRESA}: Shopee/MT (vendas por SKU)", key=f"{EMPRESA}_VND")

        st.markdown("")
        if st.button(f"Gerar Compra — {EMPRESA}", type="primary"):
            try:
                base_dir = os.path.dirname(os.path.abspath(__file__))
                cat_path_default = os.path.join(base_dir, "Padrao_produtos.xlsx")
                cat_path = st.session_state.get("padrao_override_path", cat_path_default)
                cat = carregar_padrao_produtos(cat_path)

                uploads = [full_file, fisico_file, vendas_file]
                dfs = []
                tipos_presentes = set()
                for upf in uploads:
                    if upf is None:
                        continue
                    df_raw = load_any_table(upf)
                    t = mapear_tipo(df_raw)
                    if t == "DESCONHECIDO":
                        st.error(f"Arquivo '{upf.name}' não reconhecido.")
                        st.stop()
                    dfs.append((t, mapear_colunas(df_raw, t)))
                    tipos_presentes.add(t)

                faltantes = {"FULL","FISICO","VENDAS"} - tipos_presentes
                if faltantes:
                    st.error(f"Entradas inválidas ({EMPRESA}). Faltou: {', '.join(sorted(faltantes))}.")
                    st.stop()

                full_df   = [df for t, df in dfs if t == "FULL"][0]
                fisico_df = [df for t, df in dfs if t == "FISICO"][0]
                vendas_df = [df for t, df in dfs if t == "VENDAS"][0]

                df_final, painel = calcular(full_df, fisico_df, vendas_df, cat, h=h, g=g, LT=LT)

                st.session_state[f"{EMPRESA}_df_final"] = df_final
                st.session_state[f"{EMPRESA}_painel"]   = painel
                st.success(f"{EMPRESA}: Cálculo concluído.")
            except Exception as e:
                st.error(str(e))
                st.stop()

        df_final = st.session_state.get(f"{EMPRESA}_df_final")
        painel   = st.session_state.get(f"{EMPRESA}_painel")

        if df_final is not None:
            # Painel
            st.subheader(f"{EMPRESA} — 📊 Painel de Estoques")
            cA, cB, cC, cD = st.columns(4)
            cA.metric("Full (un)",  f"{painel['full_unid']:,}".replace(",", "."))
            cB.metric("Full (R$)",  f"R$ {painel['full_valor']:,.2f}")
            cC.metric("Físico (un)",f"{painel['fisico_unid']:,}".replace(",", "."))
            cD.metric("Físico (R$)",f"R$ {painel['fisico_valor']:,.2f}")
            st.divider()

            # Filtro e Semanais
            st.subheader(f"{EMPRESA} — Itens")
            fornecedores = sorted(df_final["fornecedor"].fillna("").unique())
            colf1, colf2 = st.columns([2,1])
            with colf1:
                sel_fornec = st.multiselect("Filtrar por Fornecedor", fornecedores, key=f"{EMPRESA}_forn")
            with colf2:
                so_semanais = st.checkbox("Mostrar apenas SKUs semanais", key=f"{EMPRESA}_so_sem")

            mostra = df_final if not sel_fornec else df_final[df_final["fornecedor"].isin(sel_fornec)]

            # Gerenciar SKUs semanais (sem mexer na planilha)
            with st.expander(f"⚙️ {EMPRESA}: Gerenciar SKUs semanais (toggle no app)"):
                skus = sorted(df_final["SKU"].unique())
                atuais = set(st.session_state.get(f"{EMPRESA}_semanais", []))
                sel_sem = st.multiselect("Marcar como semanais", skus, default=sorted(atuais), key=f"{EMPRESA}_sem_sel")
                st.session_state[f"{EMPRESA}_semanais"] = sel_sem
                st.caption("Isso só filtra/organiza. Não altera cálculo.")

            if so_semanais:
                semanais = set(st.session_state.get(f"{EMPRESA}_semanais", []))
                mostra = mostra[mostra["SKU"].isin(semanais)]

            # Data limite
            data_criacao = dt.datetime.now().date()
            mostra = mostra.copy()
            mostra["data_limite"] = (dt.date.today() + dt.timedelta(days=int(janela_dias))).strftime("%Y-%m-%d")

            # Prévia
            with st.expander(f"🔎 Prévia {EMPRESA}"):
                st.dataframe(
                    mostra[[
                        "SKU","fornecedor","ML_60d","Shopee_60d","TOTAL_60d",
                        "Estoque_Fisico","Reserva_30d","Folga_Fisico",
                        "Necessidade","Compra_Sugerida","Preco","Valor_Compra_R$","data_limite"
                    ]],
                    use_container_width=True,
                    height=380
                )

            st.subheader(f"{EMPRESA} — Itens para comprar")
            st.dataframe(
                mostra[mostra["Compra_Sugerida"] > 0][[
                    "SKU","fornecedor","Vendas_h_ML","Vendas_h_Shopee",
                    "Estoque_Fisico","Preco","Compra_Sugerida","Valor_Compra_R$","data_limite"
                ]],
                use_container_width=True
            )
            compra_total = int(mostra["Compra_Sugerida"].sum())
            valor_total  = float(mostra["Valor_Compra_R$"].sum())
            st.success(f"{EMPRESA}: {len(mostra[mostra['Compra_Sugerida']>0])} SKUs com compra > 0 | Compra total: {compra_total} un | Valor: R$ {valor_total:,.2f}")

            colx1, colx2, colx3 = st.columns(3)
            with colx1:
                if st.checkbox(f"{EMPRESA}: Gerar XLSX (Lista_Final + Controle)", key=f"{EMPRESA}_chk_exp"):
                    try:
                        xlsx_bytes = exportar_xlsx_lista_final(mostra, h=h, params={"g": g, "LT": LT, "empresa": EMPRESA})
                        st.download_button(
                            label=f"Baixar XLSX — {EMPRESA}_Compra_{h}d.xlsx",
                            data=xlsx_bytes,
                            file_name=f"{EMPRESA}_Compra_{h}d.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            key=f"{EMPRESA}_btn_dl"
                        )
                        st.info("Planilha gerada a partir do DataFrame exibido (paridade garantida).")
                    except Exception as e:
                        st.error(f"Exportação bloqueada: {e}")
            with colx2:
                if st.button(f"Salvar plano do mês — {EMPRESA}"):
                    snap = mostra.rename(columns={"Compra_Sugerida":"Compra_Planejada"}).copy()
                    snap["empresa"] = EMPRESA
                    snap["h"] = h; snap["g"] = g; snap["LT"] = LT
                    snap["data_criacao"] = dt.datetime.now().strftime("%Y-%m-%d")
                    snap["data_limite"]  = (dt.date.today() + dt.timedelta(days=int(janela_dias))).strftime("%Y-%m-%d")
                    st.session_state[f"{EMPRESA}_snapshot"] = snap[[
                        "empresa","SKU","fornecedor","Compra_Planejada","Preco",
                        "ML_60d","Shopee_60d","TOTAL_60d","Reserva_30d","Folga_Fisico","Necessidade",
                        "h","g","LT","data_criacao","data_limite"
                    ]].reset_index(drop=True)
                    st.success(f"Plano salvo para {EMPRESA}.")
            with colx3:
                if st.button(f"Limpar plano — {EMPRESA}"):
                    st.session_state[f"{EMPRESA}_snapshot"] = None
                    st.success(f"Plano limpo para {EMPRESA}.")

# Render blocos das duas empresas
bloco_empresa("ALIVVIA", tab_a)
bloco_empresa("JCA", tab_j)

st.divider()
st.header("📝 Compras Efetuadas (manual) e Saldo por empresa")

def editor_compras(EMPRESA: str):
    st.subheader(f"{EMPRESA} — Compras Efetuadas (manual)")
    dfc = st.session_state.get(f"{EMPRESA}_compras")
    if dfc is None or dfc.empty:
        dfc = pd.DataFrame(columns=["data_compra","empresa","fornecedor","SKU","quantidade","obs"])
    # defaults
    hoje = dt.date.today().strftime("%Y-%m-%d")
    dfc = dfc.copy()
    if "data_compra" not in dfc.columns: dfc["data_compra"] = hoje
    if "empresa" not in dfc.columns: dfc["empresa"] = EMPRESA

    st.caption("Adicione/edite manualmente as linhas de compra.")
    edited = st.data_editor(
        dfc,
        num_rows="dynamic",
        use_container_width=True,
        column_config={
            "data_compra": st.column_config.DateColumn("data_compra", format="YYYY-MM-DD"),
            "empresa": st.column_config.TextColumn("empresa"),
            "fornecedor": st.column_config.TextColumn("fornecedor"),
            "SKU": st.column_config.TextColumn("SKU"),
            "quantidade": st.column_config.NumberColumn("quantidade", min_value=0, step=1),
            "obs": st.column_config.TextColumn("obs"),
        },
        key=f"{EMPRESA}_editor"
    )
    st.session_state[f"{EMPRESA}_compras"] = edited

    # Calcula saldo se houver plano
    snap = st.session_state.get(f"{EMPRESA}_snapshot")
    if snap is not None and not snap.empty:
        compras_ok = edited.copy()
        compras_ok = compras_ok[compras_ok["empresa"] == EMPRESA]
        compras_ok["quantidade"] = pd.to_numeric(compras_ok["quantidade"], errors="coerce").fillna(0).astype(int)
        agg = compras_ok.groupby(["empresa","SKU"], as_index=False)["quantidade"].sum().rename(columns={"quantidade":"Comprado"})
        saldo = snap.merge(agg, on=["empresa","SKU"], how="left")
        saldo["Comprado"] = saldo["Comprado"].fillna(0).astype(int)
        saldo["Saldo_A_Comprar"] = (saldo["Compra_Planejada"] - saldo["Comprado"]).clip(lower=0).astype(int)
        st.subheader(f"{EMPRESA} — Saldo a Comprar")
        st.dataframe(
            saldo[["SKU","fornecedor","Compra_Planejada","Comprado","Saldo_A_Comprar","Preco","data_criacao","data_limite"]],
            use_container_width=True
        )
        return edited, saldo
    else:
        st.info(f"Sem plano salvo para {EMPRESA}. Gere e clique 'Salvar plano do mês — {EMPRESA}'.")
        return edited, pd.DataFrame()

colL, colR = st.columns(2)
with colL:
    comp_a, saldo_a = editor_compras("ALIVVIA")
with colR:
    comp_j, saldo_j = editor_compras("JCA")

st.subheader("📤 Exportar Plano/Compras/Saldo — ambas empresas")
if st.button("Exportar XLSX consolidado (abas separadas por empresa)"):
    snaps = {"ALIVVIA": st.session_state.get("ALIVVIA_snapshot"),
             "JCA":     st.session_state.get("JCA_snapshot")}
    compras = {"ALIVVIA": st.session_state.get("ALIVVIA_compras"),
               "JCA":     st.session_state.get("JCA_compras")}
    saldos  = {"ALIVVIA": saldo_a, "JCA": saldo_j}
    x = exportar_plano_compras_saldo(snaps, compras, saldos)
    st.download_button("Baixar Plano_Compras_Saldo.xlsx", x,
                       file_name="Plano_Compras_Saldo.xlsx",
                       mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

st.caption("© Alivvia — simples, robusto e auditável. Empresas separadas, sem misturar estoques nem compras.")
