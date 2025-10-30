# ordem_compra.py
# Modulo de Ordem de Compra (OC) - robusto contra falta de estado e oc_id

import os
import json
import datetime as dt
from typing import List, Dict
import pandas as pd
import streamlit as st

BASE_OC_DIR = "ordens_compra"

def _ensure_dir(p: str):
    os.makedirs(p, exist_ok=True)

def _dir_ano_empresa(ano: int, emp: str) -> str:
    p = os.path.join(BASE_OC_DIR, str(ano), emp.upper())
    _ensure_dir(p)
    return p

def _next_seq_for_today(emp: str) -> int:
    today = dt.datetime.now().strftime("%Y%m%d")
    ano = int(dt.datetime.now().strftime("%Y"))
    pasta = _dir_ano_empresa(ano, emp)
    seq = 1
    if os.path.isdir(pasta):
        for fn in os.listdir(pasta):
            if fn.startswith(f"OC-{emp.upper()}-{today}-") and fn.endswith(".json"):
                try:
                    parte = fn.split(f"OC-{emp.upper()}-{today}-", 1)[1].replace(".json","")
                    val = int(parte)
                    if val >= seq:
                        seq = val + 1
                except:
                    pass
    return seq

def gerar_oc_id(emp: str) -> str:
    emp = (emp or "").upper().strip()
    data = dt.datetime.now().strftime("%Y%m%d")
    seq = _next_seq_for_today(emp)
    return f"OC-{emp}-{data}-{seq:04d}"

def _oc_path(oc_id: str) -> str:
    try:
        parts = oc_id.split("-")
        emp = parts[1].upper()
        date = parts[2]
        ano = int(date[:4])
    except:
        ano = int(dt.datetime.now().strftime("%Y"))
        emp = "ALIVVIA"
    pasta = _dir_ano_empresa(ano, emp)
    return os.path.join(pasta, f"{oc_id}.json")

# --------------- Estado 100% garantido ---------------
def _ensure_oc_state():
    ss = st.session_state
    if "oc_cesta" not in ss or not isinstance(ss.get("oc_cesta"), dict):
        ss["oc_cesta"] = {"ALIVVIA": [], "JCA": []}
    else:
        ss["oc_cesta"].setdefault("ALIVVIA", [])
        ss["oc_cesta"].setdefault("JCA", [])
    ss.setdefault("oc_edicao_atual", None)
    ss.setdefault("oc_cache_lista", [])
    ss.setdefault("oc_logo", "")

_ensure_oc_state()
# -----------------------------------------------------

def limpar_cesta(emp: str):
    _ensure_oc_state()
    st.session_state["oc_cesta"][emp.upper()] = []

def adicionar_itens_cesta(emp: str, df_itens: pd.DataFrame):
    _ensure_oc_state()
    emp = emp.upper()
    if df_itens is None or df_itens.empty:
        st.warning("Nenhum item selecionado para enviar a Ordem de Compra.")
        return

    def col_ok(d, nome, alts):
        if nome in d.columns: return nome
        for a in alts:
            if a in d.columns: return a
        return None

    sku_c   = col_ok(df_itens, "SKU", [])
    forn_c  = col_ok(df_itens, "fornecedor", [])
    preco_c = col_ok(df_itens, "Preco", ["Preco (R$)", "Preço", "Preco_R$"])
    compra_c= col_ok(df_itens, "Compra_Sugerida", ["Compra_ALIVVIA","Compra_JCA","Compra_Total","Compra"])
    valor_c = col_ok(df_itens, "Valor_Compra_R$", ["Total (R$)","Valor_Total","Valor"])

    itens = []
    for _, r in df_itens.iterrows():
        sku = str(r.get(sku_c, "")).strip()
        if not sku:
            continue
        forn = str(r.get(forn_c, "") or "").strip()
        preco = float(pd.to_numeric(r.get(preco_c, 0), errors="coerce") or 0)
        qtd   = int(pd.to_numeric(r.get(compra_c, 0), errors="coerce") or 0)
        val   = float(pd.to_numeric(r.get(valor_c, preco*qtd), errors="coerce") or (preco*qtd))
        if qtd <= 0:
            continue
        itens.append({
            "SKU": sku,
            "fornecedor": forn,
            "preco": preco,
            "qtd_comprada": qtd,
            "valor_total": round(val, 2),
            "nf_entregue": None,
            "qtd_recebida": None,
            "obs_receb": "",
            "descricao": ""
        })
    if not itens:
        st.info("Nenhum item com quantidade > 0 para adicionar.")
        return

    st.session_state["oc_cesta"][emp] = st.session_state["oc_cesta"][emp] + itens
    st.success(f"{len(itens)} item(ns) adicionados a cesta da {emp}.")

def salvar_oc_json(oc: Dict) -> str:
    oc_id = oc.get("oc_id") or gerar_oc_id(oc.get("empresa", "ALIVVIA"))
    oc["oc_id"] = oc_id
    agora = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if "criado_em" not in oc: oc["criado_em"] = agora
    oc["atualizado_em"] = agora
    if "status" not in oc:
        oc["status"] = "Finalizada" if oc.get("finalizada") else "Rascunho"
    p = _oc_path(oc_id)
    _ensure_dir(os.path.dirname(p))
    with open(p, "w", encoding="utf-8") as f:
        json.dump(oc, f, ensure_ascii=False, indent=2)
    return oc_id

def carregar_oc(oc_id: str) -> Dict:
    p = _oc_path(oc_id)
    with open(p, "r", encoding="utf-8") as f:
        return json.load(f)

def listar_ocs(emp: str = None, status: List[str] = None) -> List[Dict]:
    out = []
    if not os.path.isdir(BASE_OC_DIR):
        return []

    for ano in sorted(os.listdir(BASE_OC_DIR)):
        p_ano = os.path.join(BASE_OC_DIR, ano)
        if not os.path.isdir(p_ano):
            continue

        for emp_dir in os.listdir(p_ano):
            if emp and emp_dir.upper() != emp.upper():
                continue

            p_emp = os.path.join(p_ano, emp_dir)
            if not os.path.isdir(p_emp):
                continue

            for fn in os.listdir(p_emp):
                if not (fn.endswith(".json") and fn.startswith(f"OC-{emp_dir.upper()}-")):
                    continue
                fpath = os.path.join(p_emp, fn)
                try:
                    with open(fpath, "r", encoding="utf-8") as f:
                        d = json.load(f)
                except Exception:
                    continue

                if not isinstance(d, dict):
                    continue
                if "oc_id" not in d or not d.get("oc_id"):
                    d["oc_id"] = os.path.splitext(fn)[0]  # OC-EMP-YYYYMMDD-####

                if status and d.get("status") not in status:
                    continue

                out.append(d)

    out.sort(key=lambda x: (x.get("criado_em",""), x.get("oc_id","")), reverse=True)
    return out

def _css_impressao():
    return """
    <style>
    @media print { @page { size: A4; margin: 14mm; } }
    .oc-wrap { font-family: Arial, sans-serif; color:#000; }
    .oc-hdr { border-bottom:1px solid #000; padding-bottom:8px; margin-bottom:12px; display:flex; align-items:center; }
    .oc-logo { width:100px; height:60px; object-fit:contain; margin-right:12px; border:1px solid #000; }
    .oc-title { font-size:18px; font-weight:bold; }
    .oc-sub { font-size:12px; color:#000; }
    table.oc { width:100%; border-collapse:collapse; margin-top:10px; }
    table.oc th, table.oc td { border:1px solid #000; padding:6px; font-size:12px; }
    table.oc th { background:#f0f0f0; font-weight:bold; }
    .rodape { margin-top:12px; font-size:11px; }
    .assin { margin-top:16px; display:flex; gap:20px; }
    .assin div { flex:1; border-top:1px solid #000; text-align:center; padding-top:6px; }
    .mono { color:#000; }
    </style>
    """

def render_html_oc(oc: Dict, logo_url: str = None) -> str:
    empresa = oc.get("empresa","")
    fornecedor = oc.get("fornecedor","")
    oc_id = oc.get("oc_id","")
    criado_em = oc.get("criado_em","")
    condicoes = oc.get("condicoes","")
    endereco = oc.get("endereco_entrega","")
    itens = oc.get("itens", [])
    rows = []
    for it in itens:
        rows.append(f"""
        <tr>
          <td>{it.get('SKU','')}</td>
          <td>{it.get('descricao','')}</td>
          <td>{it.get('preco',0):.2f}</td>
          <td>{it.get('qtd_comprada',0)}</td>
          <td></td>
          <td></td>
          <td>{it.get('valor_total',0):.2f}</td>
        </tr>""")
    total_geral = sum([float(x.get("valor_total",0) or 0) for x in itens])
    html = f"""
    <div class='oc-wrap mono'>
      <div class='oc-hdr'>
        {'<img class="oc-logo" src="'+logo_url+'" />' if logo_url else '<div class="oc-logo"></div>'}
        <div>
          <div class='oc-title'>ORDEM DE COMPRA</div>
          <div class='oc-sub'>N: {oc_id} - Empresa: {empresa} - Fornecedor: {fornecedor} - Data: {criado_em}</div>
          <div class='oc-sub'>Endereco de entrega: {endereco}</div>
          <div class='oc-sub'>Condicoes: {condicoes}</div>
        </div>
      </div>
      <table class='oc'>
        <thead>
          <tr>
            <th>SKU</th>
            <th>Descricao</th>
            <th>Preco (R$)</th>
            <th>Qtd Comprada</th>
            <th>Qtd Recebida</th>
            <th>NF (S/N)</th>
            <th>Total (R$)</th>
          </tr>
        </thead>
        <tbody>
          {''.join(rows)}
        </tbody>
      </table>
      <div class='rodape'>Total geral: <b>R$ {total_geral:.2f}</b></div>
      <div class='assin'>
        <div>Comprador</div>
        <div>Transportadora</div>
        <div>Recebido por</div>
        <div>Data/Hora</div>
      </div>
      <div class='rodape'>Aviso: conferir recebimento. Se sem NF, marcar e registrar observacao.</div>
    </div>
    """
    return _css_impressao() + html

def _export_xlsx_oc(oc: Dict):
    import io
    bio = io.BytesIO()
    itens = pd.DataFrame(oc.get("itens", []))
    with pd.ExcelWriter(bio, engine="xlsxwriter") as w:
        if itens.empty:
            itens = pd.DataFrame(columns=["SKU","fornecedor","preco","qtd_comprada","qtd_recebida","nf_entregue","valor_total","obs_receb"])
        itens.to_excel(w, sheet_name="OC_Itens", index=False)
        ws = w.sheets["OC_Itens"]
        for i, col in enumerate(itens.columns):
            width = max(12, min(40, int(itens[col].astype(str).map(len).max() if itens.shape[0] else 12) + 2))
            ws.set_column(i, i, width)
        ws.freeze_panes(1, 0)
    bio.seek(0)
    return bio

def render_tab():
    _ensure_oc_state()

    st.header("Ordem de Compra")
    st.caption("Selecione itens nas outras telas e clique em Enviar para Ordem de Compra para popular a cesta.")

    # Cesta
    colA, colB = st.columns([3,1])
    with colA:
        emp = st.radio("Empresa da cesta", ["ALIVVIA","JCA"], horizontal=True, key="oc_emp")
    with colB:
        if st.button("Limpar cesta", use_container_width=True):
            limpar_cesta(emp)
            st.info(f"Cesta da {emp} limpa.")

    cesta = st.session_state.get("oc_cesta", {}).get(emp, [])
    df_cesta = pd.DataFrame(cesta) if cesta else pd.DataFrame(columns=["SKU","fornecedor","preco","qtd_comprada","valor_total"])
    st.subheader("Itens na Cesta")
    st.dataframe(df_cesta, use_container_width=True, hide_index=True)

    # Dados da OC
    with st.expander("Dados da Ordem de Compra", expanded=True):
        fornecedor = st.text_input("Nome do fornecedor", key="oc_fornec")
        condicoes = st.text_area("Condicoes de pagamento / observacoes (opcional)", key="oc_cond")
        endereco = st.text_input("Endereco de entrega (opcional)", key="oc_end")
        logo_url = st.text_input("URL do logo (apenas na impressao, opcional)", key="oc_logo")

        colx, coly, colz = st.columns([1,1,1])
        with colx:
            finaliza = st.checkbox("Finalizar agora (pronto para impressao)", value=True)
        with coly:
            gerar_por_fornecedor = st.checkbox("Gerar 1 OC por fornecedor (se houver varios na cesta)", value=True)
        with colz:
            permitir_edicao_posterior = st.checkbox("Permitir reabrir/editar depois", value=True)

        if st.button("Gerar e salvar OC(s)", type="primary"):
            if df_cesta.empty:
                st.warning("A cesta esta vazia.")
            elif not fornecedor and not gerar_por_fornecedor:
                st.warning("Informe o fornecedor ou ative '1 OC por fornecedor'.")
            else:
                grupos = []
                if gerar_por_fornecedor:
                    base_f = df_cesta["fornecedor"].fillna("").astype(str)
                    for f in sorted(base_f.unique().tolist()):
                        sub = df_cesta[base_f == f] if f else df_cesta[base_f == ""]
                        if sub.shape[0]:
                            grupos.append((f or fornecedor or "", sub))
                else:
                    grupos.append((fornecedor, df_cesta))

                criadas = []
                for forn_nome, df_sub in grupos:
                    itens = df_sub.to_dict(orient="records")
                    for it in itens:
                        it["descricao"] = it.get("descricao","")
                    oc = {
                        "oc_id": None,
                        "empresa": emp,
                        "fornecedor": forn_nome or "",
                        "status": "Finalizada" if finaliza else "Rascunho",
                        "finalizada": bool(finaliza),
                        "itens": itens,
                        "condicoes": condicoes,
                        "endereco_entrega": endereco,
                        "permitir_edicao": permitir_edicao_posterior,
                        "historico": [{"evento": "criada", "quando": dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}],
                    }
                    oc_id = salvar_oc_json(oc)
                    criadas.append(oc_id)

                st.success("OC(s) criada(s): " + ", ".join(criadas))
                limpar_cesta(emp)

    st.markdown("---")

    # Gerenciador
    st.subheader("Ordens de Compra - Gerenciador")
    c1, c2, c3 = st.columns([1,1,1])
    with c1:
        emp_f = st.selectbox("Empresa", ["(Todas)","ALIVVIA","JCA"], index=0)
    with c2:
        status_opts = ["(Todos)","Rascunho","Finalizada","Recebida Parcial","Recebida Total","Cancelada"]
        status_f = st.selectbox("Status", status_opts, index=0)
    with c3:
        if st.button("Atualizar lista", use_container_width=True):
            pass

    emp_arg = None if emp_f == "(Todas)" else emp_f
    stts_arg = None if status_f == "(Todos)" else [status_f]
    ocs = listar_ocs(emp_arg, stts_arg)

    if not ocs:
        st.info("Nenhuma OC encontrada nos filtros.")
        return

    df_ocs = pd.DataFrame([{
        "oc_id": o.get("oc_id",""),
        "empresa": o.get("empresa",""),
        "fornecedor": o.get("fornecedor",""),
        "status": o.get("status",""),
        "itens": len(o.get("itens",[])),
        "criado_em": o.get("criado_em",""),
        "atualizado_em": o.get("atualizado_em",""),
    } for o in ocs])

    st.dataframe(df_ocs, use_container_width=True, hide_index=True, height=280)

    opcoes_ids = ["(Selecione)"] + [d.get("oc_id","") for d in ocs]
    sel_id = st.selectbox("Abrir/Editar OC", options=opcoes_ids, index=0)

    if sel_id != "(Selecione)":
        oc = next((d for d in ocs if d.get("oc_id")==sel_id), None)
        if not oc:
            st.warning("OC nao encontrada (arquivo pode ter sido movido).")
            return

        st.write(f"OC {sel_id} - {oc.get('empresa')} / {oc.get('fornecedor')} - Status: {oc.get('status')}")

        itens = pd.DataFrame(oc.get("itens",[]))
        if not itens.empty:
            itens["nf_entregue"] = itens["nf_entregue"].astype(object)
            itens["qtd_recebida"] = itens["qtd_recebida"].astype(object)
            itens["obs_receb"] = itens["obs_receb"].astype(str)

            st.markdown("Itens (edite NF e Qtde Recebida):")
            itens_edit = st.data_editor(
                itens,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "SKU": st.column_config.TextColumn("SKU", disabled=True),
                    "fornecedor": st.column_config.TextColumn("Fornecedor", disabled=True),
                    "preco": st.column_config.NumberColumn("Preco (R$)", format="%.2f", disabled=True),
                    "qtd_comprada": st.column_config.NumberColumn("Qtd Comprada", format="%d", disabled=True),
                    "valor_total": st.column_config.NumberColumn("Total (R$)", format="%.2f", disabled=True),
                    "nf_entregue": st.column_config.CheckboxColumn("NF entregue?"),
                    "qtd_recebida": st.column_config.NumberColumn("Qtd Recebida", min_value=0),
                    "obs_receb": st.column_config.TextColumn("Obs. recebimento"),
                }
            )

            colb1, colb2, colb3, colb4 = st.columns([1,1,1,1])
            with colb1:
                if st.button("Salvar alteracoes", use_container_width=True):
                    oc["itens"] = itens_edit.to_dict(orient="records")
                    oc["historico"] = oc.get("historico", []) + [{"evento":"editado", "quando": dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}]
                    salvar_oc_json(oc)
                    st.success("OC salva.")
            with colb2:
                if st.button("Dar baixa (total)", use_container_width=True):
                    oc["status"] = "Recebida Total"
                    salvar_oc_json(oc)
                    st.success("OC marcada como Recebida Total.")
            with colb3:
                if st.button("Baixa parcial", use_container_width=True):
                    oc["status"] = "Recebida Parcial"
                    salvar_oc_json(oc)
                    st.success("OC marcada como Recebida Parcial.")
            with colb4:
                if st.button("Cancelar OC", use_container_width=True):
                    oc["status"] = "Cancelada"
                    salvar_oc_json(oc)
                    st.warning("OC cancelada.")

        st.markdown("---")
        st.subheader("Imprimir (A4 monocromatico)")
        html = render_html_oc(oc, logo_url=st.session_state.get("oc_logo",""))
        st.components.v1.html(html, height=700, scrolling=True)
        if st.button("Baixar itens da OC (XLSX)"):
            bio = _export_xlsx_oc(oc)
            st.download_button(
                "Download XLSX",
                data=bio.getvalue(),
                file_name=f"{sel_id}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
