# v4_api/api_compras.py

from fastapi import FastAPI, Body
from fastapi.middleware.cors import CORSMiddleware
from typing import Any

from v4_api.supabase_client import download_file_from_supabase
from v4_api.engine_compras import calcular_compra

app = FastAPI(title="API Reposição Alivvia v4 (motor real)")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/calcular-compra")
async def api_calcular_compra(body: dict = Body(...)) -> Any:

    print("\n=== PAYLOAD RECEBIDO ===")
    print(body)
    print("========================\n")

    horizonte = body.get("horizonte", 60)
    crescimento = body.get("crescimento", 0)
    leadtime = body.get("leadTime", 0)

    arqs = body.get("arquivos", {})

    arquivos = {
        "alivvia": {},
        "jca": {}
    }

    # ALIVVIA
    for tipo, path in arqs.get("alivvia", {}).items():
        arquivos["alivvia"][tipo] = download_file_from_supabase(path)

    # JCA
    for tipo, path in arqs.get("jca", {}).items():
        arquivos["jca"][tipo] = download_file_from_supabase(path)

    # Catálogo e Kits
    arquivos["catalogo"] = download_file_from_supabase(arqs["catalogo"])
    arquivos["kits"] = download_file_from_supabase(arqs["kits"])

    resultado = calcular_compra(
        arquivos=arquivos,
        horizonte=horizonte,
        crescimento=crescimento,
        leadtime=leadtime
    )

    return resultado
