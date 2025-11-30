from supabase import create_client, Client
from io import BytesIO
import pandas as pd

SUPABASE_URL = "https://jkpbheounmdfipgxjhnu.supabase.co"
SUPABASE_KEY = "<AQUI SUA CHAVE>"

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def download_file_from_supabase(path: str):
    bucket = "uploads"
    path = path.lstrip("/")

    response = supabase.storage.from_(bucket).download(path)

    if response is None:
        return None

    buffer = BytesIO(response)

    if path.lower().endswith(".csv"):
        return pd.read_csv(buffer, sep=None, engine="python")

    if path.lower().endswith(".xlsx"):
        return pd.read_excel(buffer)

    return None
