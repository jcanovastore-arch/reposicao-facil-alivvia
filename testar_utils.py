# -*- coding: utf-8 -*-
import json
import sys
from tiny_v3_utils import estoque_geral_por_sku

def main():
    if len(sys.argv) < 3:
        print("Uso: python testar_utils.py <CONTA> <SKU>")
        sys.exit(1)

    conta = sys.argv[1]
    sku = " ".join(sys.argv[2:])

    info, dbg = estoque_geral_por_sku(conta, sku)

    print(f"\n=== {conta.upper()} | SKU={sku} ===")
    if info is None:
        print("❌ Não consegui escolher um ID para esse SKU.\n")
        print(json.dumps(dbg, indent=2, ensure_ascii=False))
        return

    print("✔ ID escolhido / debug de candidatos:")
    print(json.dumps(dbg, indent=2, ensure_ascii=False))
    print("\n📦 ESTOQUE — Depósito 'Geral':")
    print(json.dumps(info, indent=2, ensure_ascii=False))

if __name__ == "__main__":
    main()
