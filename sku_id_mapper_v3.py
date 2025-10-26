# -*- coding: utf-8 -*-
"""
Mapa SKU -> ID (Tiny v3), com override manual e conferência do depósito 'Geral'.

Uso:
  python sku_id_mapper_v3.py ALIVVIA
  python sku_id_mapper_v3.py JCA

Requisitos:
  - tokens/<CONTA>.json   (já gerado)
  - dados/produtos_padrao.csv   (coluna 'SKU' ou 'codigo')
Saídas:
  - dados/sku_id_map_<CONTA>.csv
  - dados/sku_id_overrides.csv   (opcional; se existir, tem precedência)
"""

import csv, json, os, sys, time
import requests

BASE = "https://erp.tiny.com.br/public-api/v3"
SESS = requests.Session()

def _ensure_dirs():
    os.makedirs("tokens", exist_ok=True)
    os.makedirs("dados", exist_ok=True)

def _load_token(conta):
    path = os.path.join("tokens", f"tokens_tiny_{conta}.json")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)["access_token"]

def _headers(conta):
    return {"Authorization": f"Bearer {_load_token(conta)}"}

def _get(path, headers, params=None, max_retries=6):
    url = f"{BASE}{path}"
    for i in range(max_retries):
        r = SESS.get(url, headers=headers, params=params, timeout=60)
        if r.status_code == 429:
            time.sleep(min(2 ** i, 15))
            continue
        r.raise_for_status()
        return r.json()
    r.raise_for_status()  # se ainda falhar

def _listar_produtos_por_sku(conta, sku, max_pages=60):
    """Lista produtos aplicando filtro local de match exato do 'codigo'."""
    headers = _headers(conta)
    page, limit = 1, 100
    achados = []
    sku_norm = (sku or "").strip().upper()
    while page <= max_pages:
        js = _get("/produtos", headers, params={"sku": sku, "page": page, "limit": limit})
        itens = js.get("itens") or []
        if not itens:
            break
        # filtro local: somente codigo == SKU (case-insensitive)
        for p in itens:
            cod = (p.get("codigo") or "").strip().upper()
            if cod == sku_norm:
                achados.append(p)
        # se a página veio com menos do que o limit, acabou
        if len(itens) < limit:
            break
        page += 1
    return achados

def _estoque_geral_por_id(conta, prod_id):
    """Retorna dicionário com saldo/disponível do depósito 'Geral'."""
    headers = _headers(conta)
    js = _get(f"/estoque/{prod_id}", headers)
    deposito_geral = {"saldo": 0, "reservado": 0, "disponivel": 0}
    for dep in js.get("depositos") or []:
        nome = (dep.get("nome") or "").strip().lower()
        descons = bool(dep.get("desconsiderar", False))
        if nome == "geral" and not descons:
            deposito_geral["saldo"] = dep.get("saldo", 0)
            deposito_geral["reservado"] = dep.get("reservado", 0)
            deposito_geral["disponivel"] = dep.get("disponivel", 0)
            break
    return {
        "produto": {
            "id": js.get("id"),
            "nome": js.get("nome"),
            "codigo": js.get("codigo"),
        },
        "deposito_geral": deposito_geral
    }

def _carregar_skus():
    """Lê dados/produtos_padrao.csv e retorna lista de SKUs únicos."""
    path = os.path.join("dados", "produtos_padrao.csv")
    if not os.path.isfile(path):
        raise FileNotFoundError(
            "Planilha CSV não encontrada em: dados/produtos_padrao.csv\n"
            "Crie a pasta 'dados' e salve 'produtos_padrao.csv' com a coluna SKU ou codigo."
        )
    skus = []
    with open(path, newline="", encoding="utf-8-sig") as f:
        rd = csv.DictReader(f)
        cols = [c.strip().lower() for c in rd.fieldnames]
        col_sku = "sku" if "sku" in cols else ("codigo" if "codigo" in cols else None)
        if not col_sku:
            raise RuntimeError("A planilha precisa ter uma coluna 'SKU' ou 'codigo'.")
        for row in rd:
            sku = (row.get(col_sku) or "").strip()
            if sku:
                skus.append(sku)
    # únicos, preservando ordem
    seen, uniq = set(), []
    for s in skus:
        if s not in seen:
            seen.add(s)
            uniq.append(s)
    return uniq

def _carregar_overrides():
    """Lê overrides manuais (sku_id_overrides.csv). Campos: sku,product_id."""
    path = os.path.join("dados", "sku_id_overrides.csv")
    if not os.path.isfile(path):
        # cria arquivo vazio de exemplo
        with open(path, "w", newline="", encoding="utf-8") as f:
            wr = csv.writer(f)
            wr.writerow(["sku", "product_id"])  # cabeçalho
        return {}
    m = {}
    with open(path, newline="", encoding="utf-8-sig") as f:
        rd = csv.DictReader(f)
        for row in rd:
            sku = (row.get("sku") or "").strip()
            pid = (row.get("product_id") or "").strip()
            if sku and pid.isdigit():
                m[sku] = int(pid)
    return m

def mapear(conta):
    _ensure_dirs()
    overrides = _carregar_overrides()
    skus = _carregar_skus()
    saida = os.path.join("dados", f"sku_id_map_{conta}.csv")

    with open(saida, "w", newline="", encoding="utf-8") as f_out:
        wr = csv.writer(f_out)
        wr.writerow(["sku", "product_id", "nome", "disponivel_geral", "candidatos", "origem"])

        for sku in skus:
            origem = "auto"
            candidatos = []
            escolhido = None

            # 1) override manual tem precedência
            if sku in overrides:
                try:
                    info = _estoque_geral_por_id(conta, overrides[sku])
                    escolhido = {
                        "id": overrides[sku],
                        "nome": info["produto"]["nome"],
                        "disponivel": info["deposito_geral"]["disponivel"],
                    }
                    origem = "override"
                except Exception as e:
                    print(f"[{conta}] SKU={sku} override {overrides[sku]} falhou: {e}")

            # 2) busca automática se não houver override válido
            if not escolhido:
                try:
                    candidatos = _listar_produtos_por_sku(conta, sku)
                    if candidatos:
                        # regra simples: primeiro candidato com match exato já é confiável
                        cand = candidatos[0]
                        pid = cand.get("id")
                        info = _estoque_geral_por_id(conta, pid)
                        escolhido = {
                            "id": pid,
                            "nome": info["produto"]["nome"],
                            "disponivel": info["deposito_geral"]["disponivel"],
                        }
                    else:
                        origem = "sem_match"
                except Exception as e:
                    origem = f"erro_busca:{e}"

            # escreve linha
            if escolhido:
                wr.writerow([
                    sku,
                    escolhido["id"],
                    (escolhido["nome"] or "").replace("\n", " ").strip(),
                    escolhido["disponivel"],
                    len(candidatos),
                    origem
                ])
                print(f"✓ {conta}  {sku:>25}  → id={escolhido['id']}  ({origem})  dispGeral={escolhido['disponivel']}")
            else:
                wr.writerow([sku, "", "", "", len(candidatos), origem])
                print(f"✗ {conta}  {sku:>25}  → NÃO MAPEADO  ({origem})")

    print(f"\n👉 Mapa salvo em: {saida}")
    print("   Se precisar corrigir algo, edite dados/sku_id_overrides.csv e rode de novo.")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python sku_id_mapper_v3.py <CONTA>\nEx.: python sku_id_mapper_v3.py ALIVVIA")
        sys.exit(1)
    conta = sys.argv[1].strip().upper()
    mapear(conta)
