# mod_compra_autom.py - MÓDULO DA TAB 2 - FIX V10.1 (Adaptação para Leitura do Disco)
# Inclui correção defensiva e adaptação para carregar os bytes do disco.

import pandas as pd
import streamlit as st
import logica_compra
import numpy as np
import os # NOVO: Para manipulação de disco
import json # NOVO: Para manipular o manifesto

# [IMPORTAÇÕES DA LOGICA_COMPRA MANTIDAS]
from logica_compra import (
    Catalogo, aggregate_data_for_conjunta_clean, load_any_table_from_bytes,
    mapear_colunas, mapear_tipo, exportar_xlsx, calcular as calcular_compra
)

# Funções de suporte para o disco (replicadas ou importadas)
def load_file_from_disk(file_path: str) -> Optional[bytes]:
    """Carrega os bytes de um arquivo do disco local."""
    if file_path and os.path.exists(file_path):
        with open(file_path, "rb") as f:
            return f.read()
    return None

def _get_required_bytes(state, empresa, slot):
    """Lê os bytes do session_state ou do disco, e salva na sessão para uso imediato."""
    data = state[empresa][slot]
    
    if data["bytes"] is not None:
        return data["bytes"] # Já está na RAM
    
    if data["path"]:
        # Tenta carregar do disco (o que a lógica de persistência fez)
        bytes_from_disk = load_file_from_disk(data["path"])
        
        if bytes_from_disk is not None:
            # Salva na RAM (session_state) para que o cálculo subsequente seja rápido
            state[empresa][slot]["bytes"] = bytes_from_disk
            return bytes_from_disk
        
    return None # Arquivo não encontrado/salvo

def render_tab2(state, h, g, LT):
    """Renderiza toda a aba 'Compra Automática'."""
    st.subheader("Gerar Compra (por empresa ou conjunta) — lógica original")

    if state.catalogo_df is None or state.kits_df is None:
        st.info("Carregue o **Padrão (KITS/CAT)** no sidebar antes de usar as abas.")
        return

    # [RESTANTE DA LÓGICA ATÉ O CÁLCULO]
    
    # 2. Lógica de Disparo (ou manutenção do estado)
    if st.button(f"Gerar Compra — {nome_estado}", type="primary"):
        state.compra_autom_data["force_recalc"] = True
    
    if nome_estado not in state.compra_autom_data or state.compra_autom_data.get("force_recalc", False):
        
        state.compra_autom_data["force_recalc"] = False
        
        # BLOCO DE CÁLCULO
        try:
            # ... (Lógica do Catalogo) ...
            cat = Catalogo(
                catalogo_simples=state.catalogo_df.rename(columns={"sku":"component_sku"}),
                kits_reais=state.kits_df
            )
            
            # --- ADAPTAÇÃO CRÍTICA PARA LER OS BYTES DO DISCO ---
            if nome_estado == "CONJUNTA":
                
                dfs = {}
                missing_conjunta_calc = []
                for emp in ["ALIVVIA", "JCA"]:
                    dados = state[emp]
                    for k, rot in [("FULL", "FULL"), ("VENDAS", "Shopee/MT"), ("ESTOQUE", "Estoque")]:
                        
                        raw_bytes = _get_required_bytes(state, emp, k) # LÊ DO DISCO SE FOR NECESSÁRIO
                        
                        if raw_bytes is None:
                            missing_conjunta_calc.append(f"{emp} {rot}")
                            continue

                        raw = raw_bytes
                        tipo = mapear_tipo(raw)
                        # ... (Restante da lógica de mapeamento e agregação) ...
                
                # ... (Restante da lógica de cálculo CONJUNTA) ...

            else: # Individual (ALIVVIA ou JCA)
                dados = state[nome_estado]

                # LÊ OS BYTES DA SESSÃO OU DO DISCO
                full_raw_bytes   = _get_required_bytes(state, nome_estado, "FULL")
                vendas_raw_bytes = _get_required_bytes(state, nome_estado, "VENDAS")
                fisico_raw_bytes = _get_required_bytes(state, nome_estado, "ESTOQUE")

                if full_raw_bytes is None or vendas_raw_bytes is None or fisico_raw_bytes is None:
                    raise RuntimeError(f"Arquivos necessários não encontrados ou não salvos para {nome_estado}. Por favor, verifique a aba 'Dados das Empresas'.")
                
                full_raw = full_raw_bytes; vendas_raw = vendas_raw_bytes; fisico_raw = fisico_raw_bytes

                t_full = mapear_tipo(full_raw); t_v = mapear_tipo(vendas_raw); t_f = mapear_tipo(fisico_raw)
                if t_full != "FULL" or t_v != "VENDAS" or t_f != "FISICO":
                     raise RuntimeError("Um ou mais arquivos (FULL/VENDAS/FISICO) estão com formato incorreto.")

                full_df   = mapear_colunas(full_raw, t_full)
                vendas_df = mapear_colunas(vendas_raw, t_v)
                fisico_df = mapear_colunas(fisico_raw, t_f)
                nome_empresa_calc = nome_estado
            
            # ... (Restante do cálculo principal e salvamento no estado) ...

        except Exception as e:
            # ... (Tratamento de erro) ...
            st.error(str(e))
            
    # [RESTANTE DO CÓDIGO (RENDERIZAÇÃO DE RESULTADOS E BOTÕES)]