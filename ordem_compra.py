# ordem_compra.py - Módulo de Lógica de Ordem de Compra (V11.0 - Sincronização Total)
# - FIX: Corrige KeyError: 'Compra_Sugerida' (cesta vazia) (V10.14)
# - NOVO: Formulário "Adicionar Item Manual" com auto-preço (V10.15)
# - NOVO: Lógica de "Auto-Download" (botão aparece após salvar)
# - Mantém (V10.8) Persistência via SQLite

import json
import datetime as dt
import os
import sqlite3
from typing import Dict, List, Any

import streamlit as st
import pandas as pd
import numpy as np

# --- 1. CONFIGURAÇÃO DE PERSISTÊNCIA SQLITE ---
DB_FILE = "controle_ocs.db"
STATUS_PENDENTE = "PENDENTE"
STATUS_BAIXADA = "BAIXADA"
STATUS_CANCELADA = "CANCELADA"

# --- 2. CONFIGURAÇÕES GERAIS E IMPRESSÃO ---
LOGO_URLS = {
    "ALIVVIA": "https://i.imgur.com/bWJ6t4D.png",
    "JCA": "https://i.imgur.com/kH1yC7j.png"
}
DADOS_EMPRESAS = {
    "ALIVVIA": {"nome": "ALIVVIA COMÉRCIO LTDA", "cnpj": "XX.XXX.XXX/0001-XX", "endereco": "Rua A, 100 - Cidade/SP"},
    "JCA": {"nome": "JCA COMÉRCIO E DISTRIBUIÇÃO", "cnpj": "YY.YYY.YYY/0001-YY", "endereco": "Rua B, 200 - Cidade/SP"},
}

# --- CONEXÃO SQLITE ---
def _get_db_connection() -> sqlite3.Connection:
    """Cria e/ou conecta ao banco de dados e garante que a tabela exista."""
    try:
        conn = sqlite3.connect(DB_FILE)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS ordens_compra (
                OC_ID TEXT PRIMARY KEY,
                EMPRESA TEXT,
                FORNECEDOR TEXT,
                DATA_OC TEXT,
                DATA_PREVISTA TEXT,
                CONDICAO_PGTO TEXT,
                VALOR_TOTAL_R NUMERIC,
                STATUS TEXT,
                ITENS_JSON TEXT,
                ITENS_COUNT INTEGER
            )
        """)
        conn.commit()
        return conn
    except Exception as e:
        st.error(f"Erro Crítico de Conexão com o Banco de Dados: {e}")
        # Não faz st.stop() para permitir testes offline/sem DB
        raise RuntimeError(f"Erro no DB: {e}")


# --- GESTÃO DA CESTA (RAM) ---
def _init_cesta():
    st.session_state.setdefault("oc_cesta_itens", {"ALIVVIA": [], "JCA": []})
    st.session_state.setdefault("oc_just_saved_html", None)
    st.session_state.setdefault("oc_just_saved_id", None)


def adicionar_itens_cesta(empresa: str, df: pd.DataFrame):
    """Adiciona itens à cesta (lógica V10.10)."""
    _init_cesta()
    cesta_atual = st.session_state.oc_cesta_itens.get(empresa, [])
    # Garante que df contenha as colunas essenciais
    COLUNAS_ESSENCIAIS = ["SKU", "fornecedor", "Preco", "Compra_Sugerida", "Valor_Compra_R$"]
    for col in COLUNAS_ESSENCIAIS:
        if col not in df.columns:
            raise ValueError(f"DF de itens da cesta deve ter a coluna '{col}'.")

    itens_para_processar = df.to_dict("records")
    sku_existente_map = {item["SKU"]: item for item in cesta_atual}

    for novo_item in itens_para_processar:
        sku = str(novo_item["SKU"])
        qtd_nova = int(novo_item.get("Compra_Sugerida", 0))
        preco = float(novo_item.get("Preco", 0.0))

        if sku in sku_existente_map:
            existente = sku_existente_map[sku]
            existente["Compra_Sugerida"] += qtd_nova
            existente["Valor_Compra_R$"] = round(existente["Compra_Sugerida"] * preco, 2)
        else:
            cesta_atual.append({
                "SKU": sku,
                "fornecedor": str(novo_item.get("fornecedor", "N/A")),
                "Preco": preco,
                "Compra_Sugerida": qtd_nova,
                "Valor_Compra_R$": round(qtd_nova * preco, 2)
            })
    st.session_state.oc_cesta_itens[empresa] = cesta_atual


def adicionar_item_manual_cesta(empresa: str, fornecedor: str, sku: str, preco: float, qtd: int):
    """Adiciona um item manualmente (V10.15)"""
    _init_cesta()
    cesta_atual = st.session_state.oc_cesta_itens.get(empresa, [])
    
    item_existente = next((item for item in cesta_atual if item["SKU"] == sku), None)
    
    if item_existente:
        item_existente["Compra_Sugerida"] += qtd
        item_existente["Valor_Compra_R$"] = round(item_existente["Compra_Sugerida"] * item_existente["Preco"], 2)
    else:
        cesta_atual.append({
            "SKU": sku,
            "fornecedor": fornecedor,
            "Preco": preco,
            "Compra_Sugerida": qtd,
            "Valor_Compra_R$": round(qtd * preco, 2)
        })
    st.session_state.oc_cesta_itens[empresa] = cesta_atual


# --- LÓGICA DE BANCO DE DADOS (OC) ---
def _get_next_oc_id(empresa: str, conn: sqlite3.Connection) -> str:
    cursor = conn.cursor()
    try:
        prefixo = f"{empresa}-OC-"
        cursor.execute(f"SELECT OC_ID FROM ordens_compra WHERE OC_ID LIKE ? ORDER BY OC_ID DESC LIMIT 1", (prefixo + '%',))
        last_id = cursor.fetchone()
        next_num = 1
        if last_id:
            last_num_str = last_id[0].split('-')[-1]
            if last_num_str.isdigit():
                next_num = int(last_num_str) + 1
        return f"{prefixo}{next_num:04d}"
    except Exception:
        return f"{empresa}-OC-0001"


def salvar_oc(oc_data: Dict[str, Any]):
    conn = _get_db_connection()
    oc_id = _get_next_oc_id(oc_data["EMPRESA"], conn)
    oc_data["OC_ID"] = oc_id 

    try:
        conn.execute("""
            INSERT INTO ordens_compra VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            oc_data["OC_ID"],
            oc_data["EMPRESA"],
            oc_data["FORNECEDOR"],
            oc_data["DATA_OC"].strftime("%Y-%m-%d"),
            oc_data["DATA_PREVISTA"].strftime("%Y-%m-%d"),
            oc_data["CONDICAO_PGTO"],
            oc_data["VALOR_TOTAL_R$"],
            oc_data["STATUS"],
            json.dumps(oc_data["ITENS_JSON"], separators=(',', ':')),
            oc_data["ITENS_COUNT"]
        ))
        conn.commit()
        return oc_id
    except Exception as e:
        conn.rollback()
        raise RuntimeError(f"Falha ao salvar a OC no banco de dados: {e}")
    finally:
        conn.close()


# --- FUNÇÃO DE IMPRESSÃO ---
def gerar_html_oc(oc_data: Dict[str, Any]) -> str:
    """Gera o HTML de uma OC para impressão/download."""
    itens = oc_data.get("ITENS_JSON", []) # Assume que é uma lista de dicts
    if isinstance(itens, str):
        try: itens = json.loads(itens)
        except: itens = []
        
    itens_html = ""
    for item in itens:
        itens_html += f"""
        <tr>
            <td style="width: 15%;">{item.get('SKU', '-')}</td>
            <td style="width: 30%;">{item.get('fornecedor', '-')}</td>
            <td style="width: 10%; text-align: center;">{int(item.get('Compra_Sugerida', 0))}</td>
            <td style="width: 15%; text-align: right;">R$ {float(item.get('Preco', 0.0)):.2f}</td>
            <td style="width: 15%; text-align: right; font-weight: bold;">R$ {float(item.get('Valor_Compra_R$', 0.0)):.2f}</td>
            <td style="width: 10%; background: #ddd;"></td>
            <td style="width: 10%; background: #ddd;"></td>
        </tr>
        """

    empresa = oc_data.get("EMPRESA", "ALIVVIA")
    
    # Formato de Data BR (V10.8)
    def _formatar_data(data):
        if isinstance(data, dt.date): return data.strftime("%d/%m/%Y")
        if isinstance(data, str):
            try: return dt.datetime.strptime(data, "%Y-%m-%d").strftime("%d/%m/%Y")
            except: pass
        return str(data or "-")

    data_oc_str = _formatar_data(oc_data.get("DATA_OC"))
    data_prev_str = _formatar_data(oc_data.get("DATA_PREVISTA"))

    html = f"""
    <style>
        @media print {{ @page {{ size: A4; margin: 1cm; }} }}
        .oc-container {{ font-family: Arial, sans-serif; font-size: 10pt; border: 1px solid black; padding: 10px; }}
        .oc-header {{ border-bottom: 2px solid black; display: flex; justify-content: space-between; margin-bottom: 15px; }}
        .oc-header img {{ max-width: 150px; height: auto; }}
        .oc-items table {{ width: 100%; border-collapse: collapse; margin-top: 15px; }}
        .oc-items th, .oc-items td {{ border: 1px solid black; padding: 5px; }}
        .oc-items th {{ background-color: #ccc; }}
        .oc-footer {{ margin-top: 30px; border-top: 1px solid black; padding-top: 10px; }}
    </style>
    <div class='oc-container'>
        <div class='oc-header'>
            <div style="width: 50%;">
                <img src="{LOGO_URLS.get(empresa, '')}" alt="Logo Empresa">
                <p style="font-size: 8pt;">{DADOS_EMPRESAS.get(empresa, {}).get('nome', 'N/A')}<br>CNPJ: {DADOS_EMPRESAS.get(empresa, {}).get('cnpj', 'N/A')}</p>
            </div>
            <div style="text-align: right; width: 50%;">
                <h2 style="margin: 0;">ORDEM DE COMPRA</h2>
                <p style="font-size: 14pt;">Nº: <b>{oc_data["OC_ID"]}</b></p>
                <p>Data Emissão: {data_oc_str}</p>
            </div>
        </div>
        <p><strong>FORNECEDOR:</strong> {oc_data.get("FORNECEDOR", '-')}</p>
        <p><strong>DATA PREVISTA:</strong> {data_prev_str}</p>
        <div class='oc-items'>
            <table>
                <thead>
                    <tr>
                        <th>SKU</th><th>FORNECEDOR</th><th>QTD. PEDIDA (A)</th>
                        <th>PREÇO UN. (R$)</th><th>VALOR TOTAL (R$)</th>
                        <th style="background: #999;">QTD. CHEGOU (B)</th>
                        <th style="background: #999;">NF JUNTO?</th>
                    </tr>
                </thead>
                <tbody>{itens_html}</tbody>
            </table>
        </div>
        <div class='oc-footer'>
            <p style="text-align: right; font-weight: bold;">TOTAL OC: R$ {float(oc_data.get('VALOR_TOTAL_R$', 0.0)):,.2f}</p>
            <p>Assinatura Recebedor: _________________________</p>
        </div>
    </div>
    """
    return html

# --- FUNÇÃO PRINCIPAL DA ABA DE GERAÇÃO OC ---
def display_oc_interface(state):
    _init_cesta()
    
    # Lógica de Auto-Download (V10.15)
    if state.oc_just_saved_html:
        oc_id = state.oc_just_saved_id
        html_content = state.oc_just_saved_html
        
        st.success(f"🎉 Ordem de Compra **{oc_id}** salva com sucesso!")
        with st.expander("Baixar / Imprimir OC", expanded=True):
            st.download_button(
                label=f"💾 Baixar HTML da OC ({oc_id})",
                data=html_content,
                file_name=f"OC_{oc_id}.html",
                mime="text/html",
            )
        
        state.oc_just_saved_html = None
        state.oc_just_saved_id = None
    
    empresa = st.radio("Empresa para OC", ["ALIVVIA", "JCA"], horizontal=True, key="oc_emp_radio")
    cesta_itens = state.oc_cesta_itens.get(empresa, [])

    if state.catalogo_df is None or state.catalogo_df.empty:
        st.error("Catálogo (KITS/CAT) não carregado. Carregue no sidebar para usar a Ordem de Compra.")
        return
        
    df_catalogo = state.catalogo_df.copy()
    if "Preco" not in df_catalogo.columns: df_catalogo["Preco"] = 0.0
    if "fornecedor" not in df_catalogo.columns: df_catalogo["fornecedor"] = "N/A"
    
    df_catalogo["fornecedor"] = df_catalogo["fornecedor"].fillna("N/A").astype(str)
    df_catalogo["Preco"] = pd.to_numeric(df_catalogo["Preco"], errors='coerce').fillna(0.0)
    
    cat_fornecedores = sorted(df_catalogo["fornecedor"].unique().tolist())
    cat_fornecedores = [f for f in cat_fornecedores if f and f != "N/A"]
    
    
    # Adicionar Item Manual
    with st.expander("Adicionar Item Manual (Auto-Preço)"):
        with st.form(key="form_add_manual"):
            c1, c2, c3, c4 = st.columns([2, 3, 1, 1])
            
            forn_manual = c1.selectbox("Fornecedor", options=cat_fornecedores, key="manual_forn")
            
            if forn_manual:
                skus_do_forn = df_catalogo[df_catalogo["fornecedor"] == forn_manual]["sku"].unique().tolist()
            else:
                skus_do_forn = []
            
            sku_manual = c2.selectbox("SKU (filtrado por fornecedor)", options=skus_do_forn, key="manual_sku")
            
            preco_manual = 0.0
            if sku_manual:
                preco_manual = df_catalogo[df_catalogo["sku"] == sku_manual]["Preco"].values[0]
                
            c3.number_input("Preço (Auto)", value=preco_manual, disabled=True, key="manual_preco")
            qtd_manual = c4.number_input("Qtd.", min_value=1, value=1, step=10, key="manual_qtd")
            
            submitted_manual = st.form_submit_button("Adicionar Item à Cesta")
            
            if submitted_manual:
                if not forn_manual or not sku_manual or qtd_manual <= 0:
                    st.warning("Preencha Fornecedor, SKU e Quantidade.")
                else:
                    adicionar_item_manual_cesta(empresa, forn_manual, sku_manual, preco_manual, qtd_manual)
                    st.success(f"Adicionado: {qtd_manual}x {sku_manual}")
                    st.rerun()

    # Cesta de Itens
    df_cesta = pd.DataFrame(cesta_itens)
    colunas_editor = ["fornecedor", "SKU", "Qtd_Comprar", "Preco", "Valor_Total"]

    if not df_cesta.empty:
        # Prepara colunas para o editor (FIX V10.14)
        df_cesta["Qtd_Comprar"] = df_cesta.get("Compra_Sugerida", 0) # Usa .get para evitar KeyError se coluna sumir
        df_cesta["Valor_Total"] = df_cesta.get("Valor_Compra_R$", 0.0) # Usa .get para evitar KeyError
        df_cesta = df_cesta.fillna({"fornecedor": "", "SKU": "", "Qtd_Comprar": 0, "Preco": 0.0, "Valor_Total": 0.0})
        df_cesta_display = df_cesta[[col for col in colunas_editor if col in df_cesta.columns]].copy()
    else:
        st.info("🛒 A Cesta está vazia. Adicione itens (manualmente acima) ou nas abas 2 e 3.")
        df_cesta_display = pd.DataFrame(columns=colunas_editor)

    st.subheader(f"Cesta de Itens para {empresa} ({len(df_cesta_display)} SKUs)")

    # Seleção de Fornecedor para a OC (V10.15)
    df_final_para_salvar = df_cesta_display.copy()
    df_final_para_salvar["fornecedor"] = df_final_para_salvar["fornecedor"].astype(str)
    
    fornecedores_cesta = sorted(df_final_para_salvar["fornecedor"].astype(str).str.strip().unique().tolist())
    fornecedores_cesta = [f for f in fornecedores_cesta if f and f != "N/A"]

    if not fornecedores_cesta:
        st.warning("Nenhum fornecedor encontrado nos itens da cesta. Adicione itens para salvar a OC.")
        fornec_selecionado = None
    else:
        fornec_selecionado = st.selectbox(
            "Selecione o Fornecedor para esta OC (Um por vez)", 
            options=fornecedores_cesta,
            key=f"fornec_select_{empresa}"
        )

    with st.form(key="form_gerar_oc"):
        st.markdown("### 1. Revisão e Detalhes")

        df_editado = st.data_editor(
            df_cesta_display,
            use_container_width=True, hide_index=True,
            num_rows="dynamic",
            column_config={
                "fornecedor": st.column_config.TextColumn("Fornecedor", disabled=True),
                "SKU": st.column_config.TextColumn("SKU", disabled=True),
                "Qtd_Comprar": st.column_config.NumberColumn("Qtd. Comprar", min_value=0, format="%d"),
                "Preco": st.column_config.NumberColumn("Preço Unitário", format="R$ %.2f", disabled=False),
                "Valor_Total": st.column_config.NumberColumn("Valor Total", format="R$ %.2f", disabled=True),
            },
            key=f"editor_cesta_final_{empresa}"
        )
    
        # Recalcula e filtra os dados do editor
        df_final = df_editado.copy()
        df_final["Qtd_Comprar"] = pd.to_numeric(df_final["Qtd_Comprar"], errors="coerce").fillna(0).astype(int)
        df_final["Preco"] = pd.to_numeric(df_final["Preco"], errors="coerce").fillna(0.0)
        df_final["Valor_Total"] = (df_final["Qtd_Comprar"] * df_final["Preco"]).round(2)
        
        if fornec_selecionado:
            df_oc_final = df_final[df_final["fornecedor"] == fornec_selecionado].copy()
            valor_total_oc_final = df_oc_final["Valor_Total"].sum()
        else:
            df_oc_final = pd.DataFrame()
            valor_total_oc_final = 0.0
            
        st.metric("VALOR TOTAL DA OC (para fornecedor selecionado)", f"R$ {valor_total_oc_final:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
        
        col1, col2 = st.columns(2)
        condicao_pgto = col1.selectbox("Condição de Pagamento", ["À Vista", "Boleto 30/60/90", "Outra"], key=f"pgto_{empresa}")
        data_prevista = col2.date_input("Data Prevista de Entrega", value=dt.date.today() + dt.timedelta(days=15), key=f"data_prev_{empresa}")

        submitted = st.form_submit_button(f"💾 SALVAR OC PARA {fornec_selecionado}", type="primary", disabled=(not fornec_selecionado or valor_total_oc_final <= 0))

        if submitted:
            with st.spinner("Gerando ID e salvando OC no Banco de Dados..."):
                itens_json = df_oc_final.rename(
                    columns={"Qtd_Comprar": "Compra_Sugerida", "Valor_Total": "Valor_Compra_R$"}
                ).to_dict("records")
                
                oc_data = {
                    "OC_ID": "TEMP", "EMPRESA": empresa, "FORNECEDOR": fornec_selecionado,
                    "DATA_OC": dt.date.today(), "DATA_PREVISTA": data_prevista,
                    "CONDICAO_PGTO": condicao_pgto, "VALOR_TOTAL_R$": valor_total_oc_final,
                    "STATUS": STATUS_PENDENTE, "ITENS_JSON": itens_json, "ITENS_COUNT": len(df_oc_final)
                }
                
                try:
                    oc_id_final = salvar_oc(oc_data)
                    oc_data["OC_ID"] = oc_id_final
                    
                    html_para_download = gerar_html_oc(oc_data)
                    state.oc_just_saved_html = html_para_download
                    state.oc_just_saved_id = oc_id_final
                    
                    # Limpa os itens salvos da cesta
                    df_restante = df_final[df_final["fornecedor"] != fornec_selecionado].copy()
                    state.oc_cesta_itens[empresa] = df_restante.rename(
                         columns={"Qtd_Comprar": "Compra_Sugerida", "Valor_Total": "Valor_Compra_R$"}
                    ).to_dict("records")
                    
                    st.balloons()
                    st.rerun()
                except Exception as e:
                    st.error(f"Falha ao salvar a OC: {e}")