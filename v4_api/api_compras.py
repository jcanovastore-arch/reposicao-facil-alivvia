from fastapi import UploadFile
from engine_compras import calcular_compra_engine
from supabase_client import download_file_from_supabase

@app.post("/calcular-compra")
async def api_calcular_compra(body: dict = Body(...)) -> Any:
    """
    Versão real do cálculo:
    - baixa arquivos do Supabase
    - processa CSV/XLSX
    - explode kits
    - une vendas
    - aplica estoques
    - retorna tabela consolidada
    """

    print("\n===============================")
    print("PAYLOAD RECEBIDO EM /calcular-compra:")
    print(body)
    print("===============================\n")

    # 1) Extrair parâmetros
    horizonte = body.get("horizonte", 60)
    crescimento = body.get("crescimento", 0)
    leadtime = body.get("leadTime", 0)
    arqs = body.get("arquivos", {})

    # 2) Baixar arquivos enviados via Supabase
    arquivos_dict = {}

    for empresa, arquivos_empresa in arqs.items():
        arquivos_dict[empresa] = {}
        for tipo, path in arquivos_empresa.items():
            conteudo = download_file_from_supabase(path)
            arquivos_dict[empresa][tipo] = conteudo

    # 3) Rodar o motor real de cálculo (engine)
    resultado = calcular_compra_engine(
        arquivos=arquivos_dict,
        horizonte=horizonte,
        crescimento=crescimento,
        leadtime=leadtime
    )

    # 4) Devolver no formato esperado pelo Lovable
    return {"resultado": resultado}
