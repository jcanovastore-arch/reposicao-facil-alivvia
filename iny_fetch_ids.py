# tiny_fetch_ids.py — Coletor Tiny V2 (com ID obrigatório & cache)
# Saída: .uploads\<EMPRESA>\ESTOQUE_TINY_TESTE\inventory.csv
# Colunas: id;sku;descricao;estoque;custo_medio
# Regras:
# - Lê SKUs da Planilha Padrão (Padrao_produtos.xlsx)
# - Resolve ID por produto.obter (codigo) com fallback pesquisa; depois usa SEMPRE por ID
# - Custo: preco_custo_medio (fallback preco_custo)
# - Estoque: SOMENTE depósito "Geral", descons != 'S', empresa-alvo (JcanovaStore/PivaStore), descontando reserva
# - Ignora KITS (tipo_variacao = P)
# - Cache de IDs em .uploads\_cache\ids_por_sku.json

import os, json, time
from pathlib import Path
import requests
import pandas as pd

# ------------------------------------------------------------
ROOT = Path(__file__).resolve().parent
BASE = "https://api.tiny.com.br/api2"

PADRAO_XLSX = ROOT / "Padrao_produtos.xlsx"
UPLOADS = ROOT / ".uploads"
CACHE_DIR = UPLOADS / "_cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)
CACHE_IDS = CACHE_DIR / "ids_por_sku.json"

EMPRESA_DO_DEPOSITO = {
    "ALIVVIA": "PivaStore",
    "JCA":     "JcanovaStore",
}

# ------------------------------------------------------------
try:
    import tomllib  # py311+
except ModuleNotFoundError:
    import tomli as tomllib

with open(ROOT / ".streamlit" / "secrets.toml", "rb") as f:
    S = tomllib.load(f)

TOKENS = {
    "ALIVVIA": S.get("tiny_alivvia", {}).get("token", "").strip(),
    "JCA":     S.get("tiny_jca", {}).get("token", "").strip(),
}

# ------------------------------------------------------------
def br2f(x):
    s = str(x or "").strip()
    s = s.replace(".", "").replace(",", ".") if "," in s else s
    try: return float(s)
    except: return 0.0

def norm(s): return (s or "").strip().upper()

def post(ep, data, tries=4, backoff=0.8):
    url = f"{BASE}/{ep}"
    data = {**data, "formato":"json"}
    for i in range(1, tries+1):
        r = requests.post(url, data=data, timeout=40)
        try:
            r.raise_for_status()
            js = r.json()
        except Exception:
            if i == tries: raise
            time.sleep(backoff*i); continue
        ret = js.get("retorno", {})
        if ret.get("status") != "OK" and str(ret.get("codigo_erro")) == "6":  # API Bloqueada
            time.sleep(backoff*i + 1.2); continue
        return ret
    return ret

def get(ep, params, tries=4, backoff=0.8):
    url = f"{BASE}/{ep}"
    params = {**params, "formato":"json"}
    for i in range(1, tries+1):
        r = requests.get(url, params=params, timeout=40)
        try:
            r.raise_for_status()
            js = r.json()
        except Exception:
            if i == tries: raise
            time.sleep(backoff*i); continue
        ret = js.get("retorno", {})
        if ret.get("status") != "OK" and str(ret.get("codigo_erro")) == "6":
            time.sleep(backoff*i + 1.2); continue
        return ret
    return ret

# ------------------------------------------------------------
def pick_col_sku(df: pd.DataFrame) -> str:
    candidatos = [c.strip().lower() for c in df.columns.astype(str)]
    preferidas = {"sku","código (sku)","codigo (sku)","codigo","código","cod","codigo (si preço)","codigosku","si preço"}
    for col in df.columns:
        if col.strip().lower() in preferidas:
            return col
    return df.columns[0]

def carregar_cache():
    if CACHE_IDS.exists():
        try:
            return json.loads(CACHE_IDS.read_text(encoding="utf-8"))
        except:
            return {}
    return {}

def salvar_cache(cache: dict):
    CACHE_IDS.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")

# ------------------------------------------------------------
def resolver_produto_por_codigo(token: str, sku: str):
    """
    Tenta produto.obter.php por 'codigo'; se vier cod=10, resolve ID via produtos.pesquisa.php (match exato)
    Retorna produto (dict) ou None
    """
    r1 = get("produto.obter.php", {"token": token, "codigo": sku})
    if r1.get("status") == "OK":
        return r1.get("produto") or {}

    if str(r1.get("codigo_erro")) != "10":
        # SKU inexistente ou outro erro
        return None

    # precisa do id -> pesquisa exata
    pagina = 1
    while pagina <= 4:
        r2 = post("produtos.pesquisa.php", {
            "token": token,
            "pesquisa": sku,
            "pagina": pagina,
            "ordenacao": "codigo",
            "situacao": "A",
            "campos": "id,codigo,nome,tipoVariacao,preco_custo"
        })
        if r2.get("status") != "OK":
            return None

        itens = r2.get("produtos") or []
        if not itens: break

        for it in itens:
            p = it.get("produto") or it
            if norm(p.get("codigo")) == norm(sku):
                pid = p.get("id")
                r3 = get("produto.obter.php", {"token": token, "id": pid})
                if r3.get("status") == "OK":
                    return r3.get("produto") or {}
                else:
                    return None
        pagina += 1

    return None

def obter_custo_por_id(token: str, pid: int) -> float:
    r = get("produto.obter.php", {"token": token, "id": pid})
    if r.get("status") != "OK":
        return 0.0
    p = r.get("produto") or {}
    cm = br2f(p.get("preco_custo_medio", 0))
    if cm > 0: return cm
    return br2f(p.get("preco_custo", 0))

def obter_estoque_geral_por_id(token: str, pid: int, empresa_alvo: str) -> float:
    r = get("produto.obter.estoque.php", {"token": token, "id": pid, "empresa": empresa_alvo})
    if r.get("status") != "OK":
        return 0.0
    prod = r.get("produto") or {}
    saldo_res = br2f(prod.get("saldoReservado", 0))
    soma = 0.0
    for item in (prod.get("depositos") or []):
        dep = item.get("deposito", {}) or {}
        if norm(dep.get("nome")) == "GERAL" and (dep.get("desconsiderar") or "").upper() != "S" and (dep.get("empresa") or "") == empresa_alvo:
            soma += br2f(dep.get("saldo", 0))
    return max(soma - saldo_res, 0.0)

# ------------------------------------------------------------
def processar_empresa(nome_emp: str, token: str, empresa_alvo: str):
    if not token:
        print(f"[{nome_emp}] sem token; pulando.")
        return 0

    if not PADRAO_XLSX.exists():
        print(f"[ERRO] Não achei {PADRAO_XLSX}")
        return 0

    dfp = pd.read_excel(PADRAO_XLSX)
    col_sku = pick_col_sku(dfp)
    skus = (
        dfp[col_sku].astype(str)
        .map(lambda s: (s or "").strip())
        .replace({"nan": ""})
        .tolist()
    )
    # únicos preservando ordem
    vistos, lista = set(), []
    for s in skus:
        if s and s not in vistos:
            vistos.add(s); lista.append(s)
    skus = lista

    cache = carregar_cache()
    registros = []

    print(f"[{nome_emp}] SKUs da Planilha: {len(skus)}")

    for i, sku in enumerate(skus, start=1):
        pid = cache.get(sku)
        produto = None

        if pid:  # temos id no cache → obter produto por id (garante tipo e nome)
            r = get("produto.obter.php", {"token": token, "id": pid})
            if r.get("status") == "OK":
                produto = r.get("produto") or {}
            else:
                pid = None  # invalida cache e cai no resolver
        if not produto:
            produto = resolver_produto_por_codigo(token, sku)

        if not produto:
            # não achou no Tiny -> registra 0 pra visibilidade
            registros.append({"id": None, "sku": sku, "descricao": "", "estoque": 0.0, "custo_medio": 0.0})
            continue

        tipo = (produto.get("tipo_variacao") or "").upper()
        if tipo == "P":
            # KIT/Pai -> ignora
            continue

        pid = produto.get("id")
        codigo = produto.get("codigo") or sku
        descricao = produto.get("nome") or ""

        # salva id no cache
        cache[sku] = pid

        # custo/estoque SEMPRE via ID
        custo = obter_custo_por_id(token, pid)
        estoque = obter_estoque_geral_por_id(token, pid, empresa_alvo)

        registros.append({
            "id": int(pid),
            "sku": codigo,
            "descricao": descricao,
            "estoque": round(float(estoque), 2),
            "custo_medio": round(float(custo), 2),
        })

        # pausa leve pra respeitar API
        time.sleep(0.12)
        if i % 25 == 0:
            print(f"[{nome_emp}] {i}/{len(skus)}…")

    salvar_cache(cache)

    out_dir = UPLOADS / nome_emp / "ESTOQUE_TINY_TESTE"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_csv = out_dir / "inventory.csv"
    pd.DataFrame(registros, columns=["id","sku","descricao","estoque","custo_medio"]).to_csv(out_csv, index=False, sep=";")
    print(f"[{nome_emp}] OK: {out_csv} ({len(registros)} linhas)")
    return len(registros)

# ------------------------------------------------------------
def main():
    total = 0
    # ALIVVIA
    print("== ALIVVIA ==")
    total += processar_empresa("ALIVVIA", TOKENS.get("ALIVVIA",""), EMPRESA_DO_DEPOSITO["ALIVVIA"])
    # JCA
    print("\n== JCA ==")
    total += processar_empresa("JCA", TOKENS.get("JCA",""), EMPRESA_DO_DEPOSITO["JCA"])
    print(f"\nConcluído. Total linhas: {total}")

if __name__ == "__main__":
    main()
