# v4_api/api_compras.py
# API final — compatível com engine novo (sem Google Sheets)

from fastapi import FastAPI, Body
from fastapi.middleware.cors import CORSMiddleware
from typing import Any, Dict

from v4_api.engine_compras import calcular_compra
from v4_api.supabase_client import download_file_from_supabase


app = FastAPI(title="API Reposição Alivvia v4 — Final")


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



# ===============================================================
#                ENDPOINT PRINCIPAL /calcular-compra
# ===============================================================

@app.post("/calcular-compra")
async def api_calcular_compra(body: dict = Body(...)) -> Any:
    """
    RECEBE DO LOVABLE:
    -------------------
    {
      horizonte: 60,
      crescimento: 0,
      leadTime: 30,
      arquivos: {
        alivvia: {full, fisico, vendas},
        jca: {full, fisico, vendas}
      }
    }

    DEVOLVE PARA O LOVABLE:
    ------------------------
    {
      ALIVVIA: { tabela, painel },
      JCA: { tabela, painel }
    }
    """

    print("\n========== PAYLOAD RECEBIDO ==========")
    print(body)
    print("======================================\n")

    horizonte = body.get("horizonte", 60)
    crescimento = body.get("crescimento", 0)
    leadtime = body.get("leadTime", 0)

    arquivos_input: Dict = body.get("arquivos", {})

    # Montar estrutura final de arquivos
    arquivos_final = {
        "alivvia": {},
        "jca": {},
    }

    # ---------------- ALIVVIA ----------------
    alivvia_dict = arquivos_input.get("alivvia", {})
    for tipo, path in alivvia_dict.items():
        arquivos_final["alivvia"][tipo] = download_file_from_supabase(path)

    # ---------------- JCA ----------------
    jca_dict = arquivos_input.get("jca", {})
    for tipo, path in jca_dict.items():
        arquivos_final["jca"][tipo] = download_file_from_supabase(path)

    # ===============================================================
    #               RODAR O MOTOR REAL DE COMPRA
    # ===============================================================

    resultado, painel = calcular_compra(
        arquivos=arquivos_final,
        horizonte=horizonte,
        crescimento=crescimento,
        leadtime=leadtime
    )

    print("\n====== RETORNO PARA O LOVABLE ======")
    print(resultado)
    print("====================================\n")

    return resultado
