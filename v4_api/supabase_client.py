from supabase import create_client, Client
from io import BytesIO
import pandas as pd

# CONFIG DO SUPABASE
SUPABASE_URL = "https://jkpbheounmdfipgxjhnu.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImprcGJoZW91bm1kZmlwZ3hqaG51Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjI1NjExNzksImV4cCI6MjA3ODEzNzE3OX0.hXZYwO_xC_LOM-a2aIfKF7Y6_gRus9WNuN0dk5UW3Ww"

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def download_file_from_supabase(path: str):
    """
    Baixa um arquivo armazenado no bucket 'uploads' e retorna DataFrame.
    Aceita CSV ou XLSX automaticamente.
    """
    bucket = "uploads"
    path = path.lstrip("/")  # remove '/' inicial caso exista

    print(f"🔽 Baixando arquivo do Supabase: {bucket}/{path}")

    # Baixar arquivo
    response = supabase.storage.from_(bucket).download(path)

    if response is None:
        print(f"❌ ERRO: Arquivo {path} não encontrado no Supabase!")
        return None

    # Transformar binário em buffer
    buffer = BytesIO(response)

    # Detectar tipo
    if path.lower().endswith(".csv"):
        return pd.read_csv(buffer, sep=None, engine="python")

    if path.lower().endswith(".xlsx"):
        return pd.read_excel(buffer)

    print(f"❌ Tipo de arquivo não suportado: {path}")
    return None
