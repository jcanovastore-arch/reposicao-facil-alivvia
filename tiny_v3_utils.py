# tiny_v3_utils.py
# Resolve SKU -> ID de forma determinística (considerando duplicidades de 'codigo')
# 1) lê/atualiza cache CSV (dados/mapa_sku_id.csv)
# 2) junta candidatos vindos de filtro por 'codigo' e paginação por 'sku'
# 3) para cada candidato busca /estoque/{idProduto} e escolhe o de MAIOR 'disponivel' no depósito "Geral"
# 4) grava o ID vencedor no cache
# Requisitos: tokens/tokens_tiny_{CONTA}.json

from __future__ import annotations
import os, json, csv, time, typing as t
import requests

BASE = "https://erp.tiny.com.br/public-api/v3"
TOKENS_DIR = "tokens"
MAPA_DIR = "dados"
MAPA_ARQ = os.path.join(MAPA_DIR, "mapa_sku_id.csv")

# ---------------- Arquivos ----------------

def _ensure_dirs() -> None:
    os.makedirs(TOKENS_DIR, exist_ok=True)
    os.makedirs(MAPA_DIR, exist_ok=True)

def _load_access_token(conta: str) -> str:
    _ensure_dirs()
    path = os.path.join(TOKENS_DIR, f"tokens_tiny_{conta}.json")
    with open(path, "r", encoding="utf-8") as f:
        js = json.load(f)
    tok = js.get("access_token")
    if not tok:
        raise RuntimeError(f"access_token ausente em {path}")
    return tok

def _hdr(conta: str) -> dict:
    return {
        "Authorization": f"Bearer {_load_access_token(conta)}",
        "Accept": "application/json"
    }

# ---------------- HTTP com backoff ----------------

def _get(url: str, headers: dict, params: dict | None = None, max_retries: int = 6) -> dict:
    backoff = 1.0
    for _ in range(max_retries):
        r = requests.get(url, headers=headers, params=params, timeout=60)
        if r.status_code == 200:
            try:
                return r.json()
            except Exception:
                raise requests.HTTPError(f"Resposta não-JSON em {url}")
        if r.status_code in (429, 500, 502, 503, 504):
            wait = float(r.headers.get("Retry-After", backoff))
            time.sleep(max(0.5, wait))
            backoff = min(backoff * 2, 30.0)
            continue
        r.raise_for_status()
    r.raise_for_status()
    raise RuntimeError("Falha HTTP inesperada")

# ---------------- Cache SKU -> ID ----------------

def _carregar_mapa_ids() -> dict[tuple[str,str], int]:
    _ensure_dirs()
    mapa: dict[tuple[str,str], int] = {}
    if not os.path.exists(MAPA_ARQ):
        return mapa
    with open(MAPA_ARQ, "r", encoding="utf-8", newline="") as f:
        rd = csv.DictReader(f)
        for row in rd:
            conta = (row.get("conta") or "").strip()
            sku = (row.get("sku") or "").strip().upper()
            try:
                pid = int(row.get("id", "") or "0")
            except Exception:
                continue
            if conta and sku and pid > 0:
                mapa[(conta, sku)] = pid
    return mapa

def _salvar_mapa_ids(mapa: dict[tuple[str,str], int]) -> None:
    _ensure_dirs()
    with open(MAPA_ARQ, "w", encoding="utf-8", newline="") as f:
        wr = csv.writer(f)
        wr.writerow(["conta","sku","id"])
        for (conta, sku), pid in sorted(mapa.items()):
            wr.writerow([conta, sku, pid])

def _atualizar_cache(conta: str, sku: str, pid: int) -> None:
    mapa = _carregar_mapa_ids()
    key = (conta, (sku or "").upper())
    if mapa.get(key) != pid:
        mapa[key] = pid
        _salvar_mapa_ids(mapa)

def _buscar_cache(conta: str, sku: str) -> int | None:
    return _carregar_mapa_ids().get((conta, (sku or "").upper()))

# ---------------- Buscas de produtos ----------------

def _buscar_produtos_por_codigo(conta: str, codigo: str) -> list[dict]:
    """
    Alguns ambientes do Tiny retornam 1 item no filtro por 'codigo' mesmo havendo duplicidade.
    Por via das dúvidas, tratamos como lista.
    """
    headers = _hdr(conta)
    params = {"codigo": codigo}
    js = _get(f"{BASE}/produtos", headers, params=params)
    itens = js.get("itens") or js.get("items") or js.get("data") or []
    alvo = (codigo or "").strip().upper()
    out = []
    for p in itens:
        cod = (p.get("codigo") or p.get("sku") or "").strip().upper()
        if cod == alvo:
            out.append(p)
    return out

def _listar_produtos_por_sku(conta: str, sku: str, limit: int = 100, max_pages: int = 100) -> list[dict]:
    headers = _hdr(conta)
    itens: list[dict] = []
    page = 1
    while page <= max_pages:
        params = {"sku": sku, "page": page, "limit": limit}
        js = _get(f"{BASE}/produtos", headers, params=params)
        page_itens = js.get("itens") or js.get("items") or js.get("data") or []
        if not page_itens:
            break
        itens.extend(page_itens)
        page += 1
    # filtra match exato de codigo
    alvo = (sku or "").strip().upper()
    out = []
    for p in itens:
        cod = (p.get("codigo") or p.get("sku") or "").strip().upper()
        if cod == alvo:
            out.append(p)
    return out

# ---------------- Estoque (depósito Geral) ----------------

def _estoque_geral_do_produto(conta: str, id_produto: int) -> dict:
    headers = _hdr(conta)
    js = _get(f"{BASE}/estoque/{id_produto}", headers, params=None)
    depositos = js.get("depositos") or []
    saldo = reservado = disponivel = 0
    for d in depositos:
        if (d.get("nome") or "").strip().lower() == "geral":
            saldo = d.get("saldo", 0) or 0
            reservado = d.get("reservado", 0) or 0
            disponivel = d.get("disponivel", 0) or 0
            break
    return {
        "produto": {"id": js.get("id"), "nome": js.get("nome"), "codigo": js.get("codigo")},
        "geral": {"saldo": saldo, "reservado": reservado, "disponivel": disponivel},
        "depositos": depositos,
    }

# ---------------- Seleção do melhor candidato ----------------

def _escolher_por_estoque_geral(conta: str, candidatos: list[dict]) -> tuple[int | None, dict]:
    """
    Busca estoque de cada candidato e escolhe o de MAIOR 'disponivel' no depósito 'Geral'.
    Caso todos empatem, escolhe o primeiro de forma estável.
    """
    analise = []
    melhor_id = None
    melhor_disp = -1

    for c in candidatos:
        try:
            pid = int(c.get("id"))
        except Exception:
            continue
        info = _estoque_geral_do_produto(conta, pid)
        disp = info["geral"]["disponivel"]
        analise.append({"id": pid, "codigo": c.get("codigo"), "nome": c.get("nome"), "disponivel_geral": disp})
        if disp > melhor_disp:
            melhor_disp = disp
            melhor_id = pid

    return melhor_id, {"analise": analise, "criterio": "maior_disponivel_geral"}

# ---------------- API pública ----------------

def escolher_id_por_sku(conta: str, sku: str) -> tuple[int | None, dict]:
    """
    0) tenta cache
    1) junta candidatos por 'codigo' e por 'sku' (match exato)
    2) escolhe pelo maior estoque 'Geral'
    3) grava cache
    """
    dbg: dict = {"sku": sku, "fonte": {"codigo": [], "sku": []}, "selecionado": None, "analise": None}

    pid_cache = _buscar_cache(conta, sku)
    if pid_cache:
        dbg["selecionado"] = {"origem": "cache", "id": pid_cache}
        return pid_cache, dbg

    cand_codigo = _buscar_produtos_por_codigo(conta, sku)
    cand_sku = _listar_produtos_por_sku(conta, sku)

    dbg["fonte"]["codigo"] = [{"id": c.get("id"), "codigo": c.get("codigo"), "nome": c.get("nome")} for c in cand_codigo]
    dbg["fonte"]["sku"] = [{"id": c.get("id"), "codigo": c.get("codigo"), "nome": c.get("nome")} for c in cand_sku]

    # união por id
    vistos, todos = set(), []
    for c in cand_codigo + cand_sku:
        try:
            pid = int(c.get("id"))
        except Exception:
            continue
        if pid in vistos:
            continue
        vistos.add(pid)
        todos.append(c)

    if not todos:
        return None, dbg

    pid, analise = _escolher_por_estoque_geral(conta, todos)
    dbg["analise"] = analise
    if pid:
        _atualizar_cache(conta, sku, pid)
        dbg["selecionado"] = {"origem": "estoque_geral", "id": pid}
        return pid, dbg

    return None, dbg

def estoque_geral_por_sku(conta: str, sku: str) -> tuple[dict | None, dict]:
    pid, dbg = escolher_id_por_sku(conta, sku)
    if not pid:
        return None, dbg
    info = _estoque_geral_do_produto(conta, pid)
    return info, dbg

# ---------------- CLI rápido ----------------

if __name__ == "__main__":
    import sys, pprint
    if len(sys.argv) < 3:
        print("uso: python tiny_v3_utils.py <CONTA> <SKU>")
        sys.exit(1)
    conta = sys.argv[1]
    sku = " ".join(sys.argv[2:])
    info, dbg = estoque_geral_por_sku(conta, sku)
    print("=== DEBUG ===")
    pprint.pprint(dbg, sort_dicts=False)
    print("\n=== ESTOQUE GERAL (depósito 'Geral') ===")
    pprint.pprint(info, sort_dicts=False)
