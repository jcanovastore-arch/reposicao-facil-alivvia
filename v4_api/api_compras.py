# v4_api/api_compras.py
# API oficial da Reposição Alivvia/JCA

from typing import Any
from fastapi import FastAPI, Body
from fastapi.middleware.cors import CORSMiddleware

from .engine_compras import calcular_compra
from .supabase_client import download_file_from_supabase


# -----------------------------------------------------------
# CRIA O APP FASTAPI
# -----------------------------------------------------------

app = FastAPI(title="API Reposição Alivvia v4")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# -----------------------------------------------------------
# HEALTH CHECK
# -----------------------------------------------------------

@app.get("/health")
def health():
    return {"status": "ok"}


# -----------------------------------------------------------
# ENDPOINT PRINCIPAL: CALCULAR COMPRA
# -----------------------------------------------------------

@app.post("/calcular-compra")
async def api_calcular_compra(body: dict = Body(...)) -> Any:

    print("\n====================================")
    print("PAYLOAD RECEBIDO EM /calcular-compra:")
    print(body)
    print("====================================\n")

    # 1) Parâmetros enviados pelo Lovable
    horizonte = body.get("horizonte", 60)
    crescimento = body.get("crescimento", 0)
    leadtime = body.get("leadTime", 0)
    arqs = body.get("arquivos", {})

    # 2) Baixar arquivos do Supabase
    arquivos = {}

    for empresa, itens in arqs.items():
        arquivos[empresa] = {}
        for tipo, path in itens.items():
            if path:
                df = download_file_from_supabase(path)
                arquivos[empresa][tipo] = df
            else:
                arquivos[empresa][tipo] = None

    # 3) Rodar o motor de cálculo
    df_final, painel = calcular_compra(
        arquivos=arquivos,
        horizonte=horizonte,
        crescimento=crescimento,
        leadtime=leadtime
    )

    # 4) Retornar resultado para o Lovable
    return {
        "tabela": df_final.to_dict(orient="records"),
        "painel": painel
    }
