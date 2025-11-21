from typing import Any
from fastapi import FastAPI, Request, Body
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="API Reposição Alivvia v4 (debug)")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
def health():
    print(">> /health foi chamado")
    return {"status": "ok"}


@app.post("/calcular-compra")
async def api_calcular_compra(body: dict = Body(...)) -> Any:
    """
    MODO DEBUG:
    - não faz cálculo
    - só mostra no terminal o que chegar
    - devolve o mesmo JSON de volta
    """
    print("\n===============================")
    print("PAYLOAD RECEBIDO EM /calcular-compra:")
    print(body)
    print("===============================\n")

    return body
