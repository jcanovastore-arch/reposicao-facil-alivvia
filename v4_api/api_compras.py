from typing import Any
from fastapi import Body
from engine_compras import calcular_compra_engine
from supabase_client import download_file_from_supabase


@app.post("/calcular-compra")
async def api_calcular_compra(body: dict = Body(...)) -> Any:

    print("\n===============================")
    print("PAYLOAD RECEBIDO EM /calcular-compra:")
    print(body)
    print("===============================\n")

    # 1) Parâmetros enviados pelo Lovable
    horizonte = body.get("horizonte", 60)
    crescimento = body.get("crescimento", 0)
    leadtime = body.get("leadTime", 0)
    arqs = body.get("arquivos", {})

    # 2) Baixar arquivos do Supabase (ALI e JCA)
    arquivos_dict = {}

    for empresa, itens in arqs.items():
        arquivos_dict[empresa] = {}
        for tipo, path in itens.items():
            conteudo = download_file_from_supabase(path)
            arquivos_dict[empresa][tipo] = conteudo

    # 3) Rodar o motor real de cálculo
    resultado_df, painel = calcular_compra_engine(
        arquivos=arquivos_dict,
        horizonte=horizonte,
        crescimento=crescimento,
        leadtime=leadtime
    )

    # 4) Enviar resposta ao Lovable
    return {
        "tabela": resultado_df.to_dict(orient="records"),
        "painel": painel
    }
