from typing import Any
from fastapi import Body
from engine_compras import calcular_compra
from supabase_client import download_file_from_supabase


@app.post("/calcular-compra")
async def api_calcular_compra(body: dict = Body(...)) -> Any:

    print("\n===============================")
    print("PAYLOAD RECEBIDO EM /calcular-compra:")
    print(body)
    print("===============================\n")

    # 1) Parâmetros
    horizonte = body.get("horizonte", 60)
    crescimento = body.get("crescimento", 0)
    leadtime = body.get("leadTime", 0)
    arqs = body.get("arquivos", {})

    # 2) Carregar arquivos ALI e JCA
    # Cada empresa vira um dicionário com: full / vendas / estoque
    arquivos = {}

    for empresa, itens in arqs.items():
        arquivos[empresa] = {}
        for tipo, path in itens.items():
            df = download_file_from_supabase(path)
            arquivos[empresa][tipo] = df

    # 3) Rodar motor de cálculo real
    df_final, painel = calcular_compra(
        arquivos=arquivos,
        horizonte=horizonte,
        crescimento=crescimento,
        leadtime=leadtime
    )

    # 4) Voltar para o Lovable em formato JSON
    return {
        "tabela": df_final.to_dict(orient="records"),
        "painel": painel
    }
