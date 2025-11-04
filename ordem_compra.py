# ordem_compra.py - Módulo de Lógica de Ordem de Compra (V10.8)
# - FIX: Habilita edição de Preço no data_editor (permite inclusão manual)
# - FIX: Corrige cálculo de Valor_Total pós-edição
# - FIX: Formata data para DD/MM/YYYY na impressão

import json
import datetime as dt
import os
import sqlite3
from typing import Dict, List, Any

import streamlit as st
import pandas as pd

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
        st.stop()

# --- GESTÃO DA CESTA (RAM) ---
def _init_cesta():
    st.session_state.setdefault("oc_cesta_itens", {"ALIVVIA": [], "JCA": []})

def adicionar_itens_cesta(empresa: str, df: pd.DataFrame):
    """Adiciona itens à cesta (lógica V10.7)."""
    _init_cesta()
    cesta_atual = st.session_state.oc_cesta_itens.get(empresa, [])
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
            oc_data["DATA_OC"].strftime("%Y-%m-%d"), # Salva no DB em formato ISO
            oc_data["DATA_PREVISTA"].strftime("%Y-%m-%d"), # Salva no DB em formato ISO
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

# --- FUNÇÃO DE IMPRESSÃO (FIX V10.8 - Data) ---
def gerar_html_oc(oc_data: Dict[str, Any]) -> str:
    itens = json.loads(oc_data.get("ITENS_JSON", "[]"))
    itens_html = ""
    for item in itens:
        itens_html += f"""
        <tr>
            <td style="width: 15%;">{item.get('SKU', '-')}</td>
            <td style="width: 30%;">{item.get('fornecedor', '-')}</td>
            <td style="width: 10%; text-align: center;">{item.get('Compra_Sugerida', 0)}</td>
            <td style="width: 15%; text-align: right;">R$ {item.get('Preco', 0.0):.2f}</td>
            <td style="width: 15%; text-align: right; font-weight: bold;">R$ {item.get('Valor_Compra_R$', 0.0):.2f}</td>
            <td style="width: 10%; background: #ddd;"></td>
            <td style="width: 10%; background: #ddd;"></td>
        </tr>
        """

    empresa = oc_data.get("EMPRESA", "ALIVVIA")
    
    # =================================================================
    # >> INÍCIO DA CORREÇÃO (V10.8) - Formato de Data BR <<
    # =================================================================
    try:
        # Tenta converter data (que vem como string YYYY-MM-DD do DB)
        data_oc_str = dt.datetime.strptime(str(oc_data["DATA_OC"]), "%Y-%m-%d").strftime("%d/%m/%Y")
    except ValueError:
        data_oc_str = str(oc_data.get("DATA_OC", "-")) # Fallback

    try:
        data_prev_str = dt.datetime.strptime(str(oc_data["DATA_PREVISTA"]), "%Y-%m-%d").strftime("%d/%m/%Y")
    except ValueError:
        data_prev_str = str(oc_data.get("DATA_PREVISTA", "-")) # Fallback
    # =================================================================
    # >> FIM DA CORREÇÃO (V10.8) <<
    # =================================================================

    html = f"""
    <style>
        /* (Estilos CSS omitidos para brevidade) */
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
                <p style="font-size: 8pt;">{DADOS_EMPRESAS[empresa]['nome']}<br>CNPJ: {DADOS_EMPRESAS[empresa]['cnpj']}</p>
            </div>
            <div style="text-align: right; width: 50%;">
                <h2 style="margin: 0;">ORDEM DE COMPRA</h2>
                <p style="font-size: 14pt;">Nº: <b>{oc_data["OC_ID"]}</b></p>
                <p>Data Emissão: {data_oc_str}</p>
            </div>
        </div>
        <p><strong>FORNECEDOR:</strong> {oc_data["FORNECEDOR"]}</p>
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
            <p style="text-align: right; font-weight: bold;">TOTAL OC: R$ {oc_data["VALOR_TOTAL_R$"]:,.2f}</p>
            <p>Assinatura Recebedor: _________________________</p>
        </div>
    </div>
    """
    return html

# --- FUNÇÃO PRINCIPAL DA ABA DE GERAÇÃO OC (FIX V10.8 - Edição) ---
def display_oc_interface(df_reposicao_final):
    _init_cesta()
    empresa = st.radio("Empresa para OC", ["ALIVVIA", "JCA"], horizontal=True, key="oc_emp_radio")
    
    cesta_itens = st.session_state.oc_cesta_itens.get(empresa, [])

    if not cesta_itens:
        st.info("🛒 A Cesta está vazia. Selecione itens na 'Compra Automática' (Tab 2).")
        return

    df_cesta = pd.DataFrame(cesta_itens)
    # Garante que as colunas existam mesmo se a cesta estiver vazia e for editada
    df_cesta["Qtd_Comprar"] = df_cesta["Compra_Sugerida"]
    df_cesta["Valor_Total"] = (df_cesta["Qtd_Comprar"] * df_cesta["Preco"]).round(2)
    df_cesta = df_cesta.fillna({"fornecedor": "", "SKU": ""}) # Prepara para adição de novas linhas

    st.subheader(f"Cesta de Itens para {empresa} ({len(df_cesta)} SKUs)")

    with st.form(key="form_gerar_oc"):
        st.markdown("### 1. Revisão e Detalhes")
        st.caption("Você pode editar 'Qtd. Comprar', 'Preço Unitário' ou adicionar novas linhas.")

        df_editado = st.data_editor(
            df_cesta[[
                "fornecedor", "SKU", "Qtd_Comprar", "Preco", "Valor_Total"
            ]],
            use_container_width=True, hide_index=True,
            num_rows="dynamic", # Permite adicionar/deletar linhas
            column_config={
                "fornecedor": st.column_config.TextColumn("Fornecedor"),
                "SKU": st.column_config.TextColumn("SKU"),
                "Qtd_Comprar": st.column_config.NumberColumn("Qtd. Comprar", min_value=0, format="%d"),
                
                # =================================================================
                # >> INÍCIO DA CORREÇÃO (V10.8) - Edição de Preço <<
                # =================================================================
                "Preco": st.column_config.NumberColumn("Preço Unitário", format="R$ %.2f", disabled=False),
                # =================================================================
                
                "Valor_Total": st.column_config.NumberColumn("Valor Total", format="R$ %.2f", disabled=True),
            },
            key=f"editor_cesta_final_{empresa}"
        )

        # =================================================================
        # >> INÍCIO DA CORREÇÃO (V10.8) - Recálculo pós-edição <<
        # =================================================================
        # Recalcula o valor total com base nos dados ATUAIS do editor
        df_final = df_editado.copy()
        df_final["Qtd_Comprar"] = pd.to_numeric(df_final["Qtd_Comprar"], errors="coerce").fillna(0).astype(int)
        df_final["Preco"] = pd.to_numeric(df_final["Preco"], errors="coerce").fillna(0.0)
        df_final["Valor_Total"] = (df_final["Qtd_Comprar"] * df_final["Preco"]).round(2)
        # =================================================================
        
        total_oc = df_final["Valor_Total"].sum()
        st.metric("VALOR TOTAL ESTIMADO DA OC", f"R$ {total_oc:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))

        fornecedores = sorted(df_final["fornecedor"].astype(str).str.strip().unique().tolist())
        fornecedores = [f for f in fornecedores if f] # Remove vazios
        
        if not fornecedores:
            st.error("Nenhum fornecedor encontrado nos itens da cesta. Adicione um fornecedor.")
            st.stop()
            
        fornec_selecionado = st.selectbox("Selecione o Fornecedor para esta OC (Um por vez)", options=fornecedores)
        
        df_oc_final = df_final[df_final["fornecedor"] == fornec_selecionado].copy()
        valor_total_oc_final = df_oc_final["Valor_Total"].sum()
        
        st.info(f"Gerando OC apenas para **{fornec_selecionado}** (R$ {valor_total_oc_final:,.2f})")
        
        col1, col2 = st.columns(2)
        condicao_pgto = col1.selectbox("Condição de Pagamento", ["À Vista", "Boleto 30/60/90", "Outra"], key=f"pgto_{empresa}")
        
        # =================================================================
        # >> INÍCIO DA CORREÇÃO (V10.8) - Data Prevista BR <<
        # =================================================================
        # A data é salva corretamente no DB, o input se adapta ao browser.
        # A mudança foi na impressão (gerar_html_oc).
        data_prevista = col2.date_input("Data Prevista de Entrega", value=dt.date.today() + dt.timedelta(days=15), key=f"data_prev_{empresa}")
        # =================================================================

        submitted = st.form_submit_button(f"💾 SALVAR OC E IMPRIMIR PARA {fornec_selecionado}", type="primary")

        if submitted:
            if valor_total_oc_final <= 0:
                st.error("Valor total da OC é R$ 0,00. Nada foi salvo.")
            else:
                with st.spinner("Gerando ID e salvando OC no Banco de Dados..."):
                    # Prepara o JSON para salvar
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
                        st.success(f"🎉 Ordem de Compra **{oc_id_final}** salva com sucesso no Banco de Dados!")
                        
                        # Limpa os itens salvos da cesta
                        st.session_state.oc_cesta_itens[empresa] = df_final[
                            df_final["fornecedor"] != fornec_selecionado
                        ].to_dict("records")
                        
                        st.balloons()
                        st.rerun() # Recarrega a página para limpar o form
                    except Exception as e:
                        st.error(f"Falha ao salvar a OC: {e}")

# --- Ponto de Entrada (V10.7) ---
def render_tab4(state):
    display_oc_interface(df_reposicao_final=None)