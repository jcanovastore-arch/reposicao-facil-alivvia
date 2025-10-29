import os, json

BASE_UPLOAD_DIR = ".uploads"

def ensure_dir(p):
    os.makedirs(p, exist_ok=True)

def disk_dir(emp: str, kind: str) -> str:
    p = os.path.join(BASE_UPLOAD_DIR, emp, kind)
    ensure_dir(p)
    return p

def disk_put(emp: str, kind: str, name: str, blob: bytes):
    p = disk_dir(emp, kind)
    with open(os.path.join(p, "file.bin"), "wb") as f:
        f.write(blob)
    with open(os.path.join(p, "meta.json"), "w", encoding="utf-8") as f:
        json.dump({"name": name}, f)

def disk_get(emp: str, kind: str):
    p = disk_dir(emp, kind)
    meta = os.path.join(p, "meta.json")
    data = os.path.join(p, "file.bin")
    if not (os.path.exists(meta) and os.path.exists(data)):
        return None
    try:
        with open(meta, "r", encoding="utf-8") as f:
            info = json.load(f)
        with open(data, "rb") as f:
            blob = f.read()
        return {"name": info.get("name","arquivo.bin"), "bytes": blob}
    except Exception:
        return None
