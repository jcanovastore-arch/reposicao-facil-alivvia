# ordem_compra.py - Módulo de Lógica de Ordem de Compra (AGORA COM SQLITE)
import json
import datetime as dt
import os
import sqlite3
from typing import Dict, List, Any

import streamlit as st
import pandas as pd

# --- 1. CONFIGURAÇÃO DE PERSISTÊNCIA SQLITE ---
# O arquivo de banco de dados será criado na sua pasta
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
        # Cria a tabela se não existir
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
    """Adiciona itens à cesta (lógica inalterada)."""
    # [Lógica da cesta inalterada e omitida para brevidade]
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
                "fornecedor": str(novo_item["fornecedor"]),
                "Preco": preco,
                "Compra_Sugerida": qtd_nova,
                "Valor_Compra_R$": round(qtd_nova * preco, 2)
            })
            
    st.session_state.oc_cesta_itens[empresa] = cesta_atual
    
def _get_next_oc_id(empresa: str, conn: sqlite3.Connection) -> str:
    """Gera o próximo ID sequencial a partir do banco de dados."""
    cursor = conn.cursor()
    try:
        prefixo = f"{empresa}-OC-"
        # Busca o maior número de OC para esta empresa
        cursor.execute(f"""
            SELECT OC_ID FROM ordens_compra 
            WHERE OC_ID LIKE ? 
            ORDER BY OC_ID DESC 
            LIMIT 1
        """, (prefixo + '%',))
        
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
    """Salva a OC no Banco de Dados SQLite."""
    conn = _get_db_connection()
    oc_id = _get_next_oc_id(oc_data["EMPRESA"], conn)
    oc_data["OC_ID"] = oc_id # Atualiza o ID na estrutura de dados

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

# --- FUNÇÃO DE IMPRESSÃO (INALTERADA) ---
def gerar_html_oc(oc_data: Dict[str, Any]) -> str:
    # [A função gerar_html_oc permanece a mesma, pois ela só usa os dados de entrada]
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
    
    html = f"""
    <style>
        /* Estilos para impressão (A4) */
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
                <p>Data Emissão: {oc_data["DATA_OC"]}</p>
            </div>
        </div>
        
        <p><strong>FORNECEDOR:</strong> {oc_data["FORNECEDOR"]}</p>
        <p><strong>DATA PREVISTA:</strong> {oc_data["DATA_PREVISTA"]}</p>
        
        <div class='oc-items'>
            <table>
                <thead>
                    <tr>
                        <th>SKU</th>
                        <th>FORNECEDOR</th>
                        <th>QTD. PEDIDA (A)</th>
                        <th>PREÇO UN. (R$)</th>
                        <th>VALOR TOTAL (R$)</th>
                        <th style="background: #999;">QTD. CHEGOU (B)</th>
                        <th style="background: #999;">NF JUNTO?</th>
                    </tr>
                </thead>
                <tbody>
                    {itens_html}
                </tbody>
            </table>
        </div>
        <div class='oc-footer'>
            <p style="text-align: right; font-weight: bold;">TOTAL OC: R$ {oc_data["VALOR_TOTAL_R$"]:,.2f}</p>
            <p>Assinatura Recebedor: _________________________</p>
        </div>
    </div>
    """
    return html


# --- FUNÇÃO PRINCIPAL DA ABA DE GERAÇÃO OC (Lógica inalterada) ---

def display_oc_interface(df_reposicao_final):
    """Renderiza a UI para criar a OC a partir da cesta."""
    # [O restante da função display_oc_interface permanece o mesmo, chamando a nova função salvar_oc]
    _init_cesta()
    empresa = st.radio("Empresa para OC", ["ALIVVIA", "JCA"], horizontal=True, key="oc_emp_radio")
    
    cesta_itens = st.session_state.oc_cesta_itens.get(empresa, [])
    
    # 1. VISUALIZAÇÃO DA CESTA
    if not cesta_itens:
        st.info("🛒 A Cesta está vazia. Selecione itens na 'Compra Automática'.")
        return

    df_cesta = pd.DataFrame(cesta_itens)
    df_cesta["Qtd_Comprar"] = df_cesta["Compra_Sugerida"]
    df_cesta["Valor_Total"] = (df_cesta["Qtd_Comprar"] * df_cesta["Preco"]).round(2)
    
    st.subheader(f"Cesta de Itens para {empresa} ({len(df_cesta)} SKUs)")
    
    with st.form(key="form_gerar_oc"):
        st.markdown("### 1. Revisão e Detalhes")
        
        df_editado = st.data_editor(
            df_cesta[[
                "fornecedor", "SKU", "Qtd_Comprar", "Preco", "Valor_Total"
            ]],
            use_container_width=True, hide_index=True,
            num_rows="dynamic",
            column_config={
                "Qtd_Comprar": st.column_config.NumberColumn("Qtd. Comprar", min_value=1, format="%d"),
                "Preco": st.column_config.NumberColumn("Preço Unitário", format="R$ %.2f", disabled=True),
                "Valor_Total": st.column_config.NumberColumn("Valor Total", format="R$ %.2f", disabled=True),
            },
            key=f"editor_cesta_final_{empresa}"
        )
        
        df_final = df_editado.copy()
        df_final["Qtd_Comprar"] = pd.to_numeric(df_final["Qtd_Comprar"], errors="coerce").fillna(0).astype(int).clip(lower=1)
        df_final["Preco"] = df_cesta["Preco"] 
        df_final["Valor_Total"] = (df_final["Qtd_Comprar"] * df_final["Preco"]).round(2)
        
        total_oc = df_final["Valor_Total"].sum()
        st.metric("VALOR TOTAL ESTIMADO DA OC", f"R$ {total_oc:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
        
        fornecedores = sorted(df_final["fornecedor"].unique().tolist())
        if not fornecedores:
            st.error("Nenhum fornecedor encontrado nos itens da cesta.")
            st.stop()
            
        fornec_selecionado = st.selectbox("Selecione o Fornecedor para esta OC (Um por vez)", options=fornecedores)
        
        df_oc_final = df_final[df_final["fornecedor"] == fornec_selecionado].copy()
        valor_total_oc_final = df_oc_final["Valor_Total"].sum()
        
        st.info(f"Gerando OC apenas para **{fornec_selecionado}** (R$ {valor_total_oc_final:,.2f})")
        
        col1, col2 = st.columns(2)
        condicao_pgto = col1.selectbox("Condição de Pagamento", ["À Vista", "Boleto 30/60/90", "Outra"], key=f"pgto_{empresa}")
        data_prevista = col2.date_input("Data Prevista de Entrega", value=dt.date.today() + dt.timedelta(days=15), key=f"data_prev_{empresa}")
        
        submitted = st.form_submit_button(f"💾 SALVAR OC E IMPRIMIR PARA {fornec_selecionado}", type="primary")

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
                    st.success(f"🎉 Ordem de Compra **{oc_id_final}** salva com sucesso no Banco de Dados!")
                    
                    # Limpa os itens salvos da cesta e recarrega
                    st.session_state.oc_cesta_itens[empresa] = [
                         item for item in st.session_state.oc_cesta_itens.get(empresa, []) 
                         if item.get("fornecedor") != fornec_selecionado
                    ]
                    st.balloons()
                    st.experimental_rerun()
                except Exception as e:
                    st.error(f"Falha ao salvar a OC: {e}")