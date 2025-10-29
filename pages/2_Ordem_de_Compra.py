import os, io, re, json, datetime as dt
import pandas as pd
import numpy as np
import streamlit as st

# =========================
# Configuração da página
# =========================
st.set_page_config(page_title="🧾 Ordem de Compra", layout="wide")
st.title("🧾 Ordem de Compra")
st.caption("Passo 1 — criar e gravar OC (JSON + XLSX) sem download automático.")

# Pastas base (já criadas por você)
BASE_OC_DIR = "ordens_compra"
LOGO_DIR = "assets/logos"

# Paleta por empresa (ALIVVIA / JCA) — logos e cores padronizados
PALETAS = {
    "ALIVVIA": {
        "prim": "#195A64",
        "sec":  "#BBE64E",
        "logo": os.path.join(LOGO_DIR, "alivvia_logo.png"),
    },
    "JCA": {
        "prim": "#6E3CBC",    # roxo padrão JCA
        "sec":  "#B497E6",    # apoio claro
        "logo": os.path.join(LOGO_DIR, "jca_logo.png"),
    },
}

# -------------------------
# Helpers utilitários
# -------------------------
def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def hoje_ano():
    return dt.datetime.now().strftime("%Y")

def hoje_data_compact():
    return dt.datetime.now().strftime("%Y%m%d")

def prox_sequencial(empresa: str, data_compact: str) -> str:
    """
    Varre a pasta do dia e retorna o próximo SEQ (4 dígitos).
    Padrão do arquivo: OC-<EMP>-<YYYYMMDD>-<SEQ4>.json/xlsx
    """
    ano = hoje_ano()
    base = os.path.join(BASE_OC_DIR, ano, empresa)
    ensure_dir(base)
    padrao = re.compile(rf"^OC-{empresa}-{data_compact}-(\d{{4}})\.(json|xlsx)$", re.IGNORECASE)
    seqs = []
    for fn in os.listdir(base):
        m = padrao.match(fn)
        if m:
            seqs.append(int(m.group(1)))
    nxt = (max(seqs) + 1) if seqs else 1
    return f"{nxt:04d}"

def numero_oc(empresa: str, data_compact: str, seq: str) -> str:
    return f"OC-{empresa}-{data_compact}-{seq}"

def caminho_ano_emp(empresa: str) -> str:
    ano = hoje_ano()
    p = os.path.join(BASE_OC_DIR, ano, empresa)
    ensure_dir(p)
    return p

def caminho_index(empresa: str) -> str:
    ano = hoje_ano()
    ensure_dir(os.path.join(BASE_OC_DIR, ano))
    return os.path.join(BASE_OC_DIR, ano, f"index_{empresa}.csv")

def carregar_index(empresa: str) -> pd.DataFrame:
    idx = caminho_index(empresa)
    if os.path.exists(idx):
        try:
            return pd.read_csv(idx, dtype=str, keep_default_na=False)
        except Exception:
            pass
    return pd.DataFrame(columns=[
        "numero_oc","empresa","fornecedor","data_emissao","itens","total","frete","status",
        "com_nf","caminho_json","caminho_xlsx","ultima_atualizacao"
    ])

def salvar_index(df: pd.DataFrame, empresa: str):
    df = df.copy()
    df = df.sort_values("data_emissao", ascending=False)
    df.to_csv(caminho_index(empresa), index=False, encoding="utf-8")

def format_currency(v):
    try:
        return f"R$ {float(v):,.2f}"
    except:
        return "R$ 0,00"

def gerar_xlsx_oc(empresa: str, numero: str, cab: dict, itens_df: pd.DataFrame) -> bytes:
    """
    Gera um XLSX simples e profissional, com paleta/cores e logo embutido.
    """
    pal = PALETAS.get(empresa, {"prim":"#333333","sec":"#DDDDDD","logo":""})
    prim = pal["prim"]; sec = pal["sec"]; logo = pal["logo"]

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as w:
        # criar worksheet manualmente (layout controlado)
        ws = w.book.add_worksheet("Ordem de Compra")
        w.sheets["Ordem de Compra"] = ws

        # Estilos
        fmt_title = w.book.add_format({"bold": True, "font_size": 16, "align": "left", "valign": "vcenter", "font_color": "#FFFFFF", "bg_color": prim})
        fmt_head  = w.book.add_format({"bold": True, "bg_color": sec, "border": 1})
        fmt_cell  = w.book.add_format({"border": 1})
        fmt_money = w.book.add_format({"num_format": "R$ #,##0.00", "border": 1})
        fmt_tot   = w.book.add_format({"bold": True, "num_format": "R$ #,##0.00", "border": 1})
        fmt_lbl   = w.book.add_format({"italic": True, "align": "right"})
        fmt_val   = w.book.add_format({"bold": True})

        # Larguras / Header
        ws.set_column(0, 0, 16)  # SKU
        ws.set_column(1, 1, 42)  # Descrição
        ws.set_column(2, 4, 14)  # Qtd, Unit, Subtotal
        ws.set_row(0, 28)
        ws.merge_range(0, 0, 0, 4, f"{empresa} — {numero}", fmt_title)

        # Logo (se existir)
        if os.path.exists(logo):
            try:
                ws.insert_image(0, 4, logo, {"x_scale": 0.35, "y_scale": 0.35, "x_offset": 6, "y_offset": 2, "object_position": 1})
            except Exception:
                pass

        # Cabeçalho textual
        row = 2
        ws.write(row, 0, "Fornecedor:", fmt_lbl); ws.write(row, 1, cab.get("fornecedor",""), fmt_val)
        ws.write(row, 2, "Data emissão:", fmt_lbl); ws.write(row, 3, cab.get("data_emissao",""), fmt_val)
        row += 1
        ws.write(row, 0, "Com NF?", fmt_lbl); ws.write(row, 1, "Sim" if cab.get("com_nf") else "Não")
        ws.write(row, 2, "Criado por:", fmt_lbl); ws.write(row, 3, cab.get("criado_por",""))
        row += 2

        # Tabela itens
        headers = ["SKU", "Descrição", "Qtd Comprada", "Preço Unitário", "Subtotal"]
        for col, h in enumerate(headers):
            ws.write(row, col, h, fmt_head)
        row += 1

        total = 0.0
        for _, r in itens_df.iterrows():
            sku = str(r.get("SKU","")).strip().upper()
            desc = str(r.get("Descricao","")).strip()
            qtd = float(r.get("Qtd", 0) or 0)
            unit = float(r.get("PrecoUnit", 0.0) or 0.0)
            sub = qtd * unit
            total += sub

            ws.write(row, 0, sku, fmt_cell)
            ws.write(row, 1, desc, fmt_cell)
            ws.write_number(row, 2, qtd, fmt_cell)
            ws.write_number(row, 3, unit, fmt_money)
            ws.write_number(row, 4, sub, fmt_money)
            row += 1

        # Frete + total
        frete = float(cab.get("frete", 0.0) or 0.0)
        row += 1
        ws.write(row, 3, "Frete:", fmt_lbl); ws.write_number(row, 4, frete, fmt_money)
        row += 1
        ws.write(row, 3, "Total Geral:", fmt_lbl); ws.write_number(row, 4, total + frete, fmt_tot)

    output.seek(0)
    return output.read()

# -------------------------
# UI — Formulário OC
# -------------------------
with st.form("form_oc"):
    colA, colB, colC = st.columns([1,1,1])
    with colA:
        empresa = st.radio("Empresa", ["ALIVVIA", "JCA"], horizontal=True)
    with colB:
        fornecedor = st.text_input("Fornecedor", placeholder="Ex.: Fornecedor XYZ Ltda")
    with colC:
        com_nf = st.checkbox("Com NF?", value=False)

    col1, col2 = st.columns([2,1])
    with col1:
        st.markdown("**Itens da OC**")
        st.caption("Adicione/edite os itens. Campos: SKU, Descricao, Qtd, PrecoUnit")
        default_rows = pd.DataFrame([{"SKU":"", "Descricao":"", "Qtd":0, "PrecoUnit":0.0}])
        itens = st.data_editor(
            default_rows,
            num_rows="dynamic",
            use_container_width=True,
            hide_index=True,
            column_config={
                "SKU": {"width": 140},
                "Descricao": {"width": 360},
                "Qtd": {"width": 100},
                "PrecoUnit": {"width": 120},
            }
        )
    with col2:
        frete = st.number_input("Frete (R$)", min_value=0.0, step=10.0, value=0.0, format="%.2f")
        criado_por = st.text_input("Criado por", value="", placeholder="Opcional")

    obs = st.text_area("Observações (opcional)", value="", height=80, placeholder="Regras especiais, prazos etc.")

    submitted = st.form_submit_button("💾 Gravar Ordem de Compra", type="primary")

if submitted:
    try:
        # Normalização de itens
        itens = itens.fillna({"SKU":"", "Descricao":"", "Qtd":0, "PrecoUnit":0.0})
        itens["SKU"] = itens["SKU"].astype(str).str.strip().str.upper()
        itens["Descricao"] = itens["Descricao"].astype(str).str.strip()
        itens["Qtd"] = pd.to_numeric(itens["Qtd"], errors="coerce").fillna(0).astype(float)
        itens["PrecoUnit"] = pd.to_numeric(itens["PrecoUnit"], errors="coerce").fillna(0.0).astype(float)
        itens = itens[itens["SKU"] != ""]

        if itens.empty:
            st.error("Adicione ao menos 1 item (SKU).")
            st.stop()

        data_compact = hoje_data_compact()
        seq = prox_sequencial(empresa, data_compact)
        num = numero_oc(empresa, data_compact, seq)

        # Cabeçalho
        cab = {
            "numero_oc": num,
            "empresa": empresa,
            "fornecedor": (fornecedor or "").strip(),
            "data_emissao": dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "com_nf": bool(com_nf),
            "frete": float(frete or 0.0),
            "criado_por": (criado_por or "").strip(),
            "observacoes": (obs or "").strip(),
            "status": "ABERTA",
        }

        # Salvar JSON
        ano_dir = caminho_ano_emp(empresa)
        json_path = os.path.join(ano_dir, f"{num}.json")
        payload = {"cabecalho": cab, "itens": itens.to_dict(orient="records")}
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

        # Salvar XLSX (layout com logo e cores)
        xlsx_bytes = gerar_xlsx_oc(empresa, num, cab, itens)
        xlsx_path = os.path.join(ano_dir, f"{num}.xlsx")
        with open(xlsx_path, "wb") as f:
            f.write(xlsx_bytes)

        # Atualizar índice (por empresa/ano)
        idx = carregar_index(empresa)
        total_itens = float((itens["Qtd"] * itens["PrecoUnit"]).sum())
        frete_v = float(frete or 0.0)
        nova = pd.DataFrame([{
            "numero_oc": num,
            "empresa": empresa,
            "fornecedor": cab["fornecedor"],
            "data_emissao": cab["data_emissao"],
            "itens": str(len(itens)),
            "total": f"{total_itens + frete_v:.2f}",
            "frete": f"{frete_v:.2f}",
            "status": cab["status"],
            "com_nf": "Sim" if cab["com_nf"] else "Não",
            "caminho_json": json_path,
            "caminho_xlsx": xlsx_path,
            "ultima_atualizacao": dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }])
        # evita duplicata
        idx = idx[idx["numero_oc"] != num]
        idx = pd.concat([nova, idx], ignore_index=True)
        salvar_index(idx, empresa)

        st.success(f"OC gravada: {num}")
        st.caption(f"Arquivos salvos: {json_path} e {xlsx_path}")

        # Botões opcionais (download manual — não é obrigatório)
        colD, colE = st.columns([1,1])
        with colD:
            st.download_button("⬇️ Baixar XLSX", data=xlsx_bytes, file_name=f"{num}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        with colE:
            st.download_button("⬇️ Baixar JSON", data=json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8"), file_name=f"{num}.json", mime="application/json")

    except Exception as e:
        st.error(f"Falha ao gravar OC: {e}")
