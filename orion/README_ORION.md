# ORION — Guia de Modularização

**Objetivo:** organizar o projeto em camadas, sem quebrar o app enquanto evoluímos o código.
Integraremos o **Plano ATHENAS** (melhorias da Ordem de Compra e filtros inteligentes) já dentro dessa estrutura.

## Pastas
- `orion/core`: utilitários reutilizáveis (normalizações, helpers).
- `orion/data`: leitura/normalização de planilhas e dados.
- `orion/tiny`: integração Tiny v3 (tokens, requests, sync).
- `orion/oc`: exportação/armazenamento da Ordem de Compra (XLSX/JSON) e conferência de recebimento.
- `orion/ui`: componentes de UI (filtros, grids, botões, métricas).

## Estratégia de migração
1. Criar estrutura (este passo) — sem alterar comportamento.
2. Mover **filtros e seleção** (ABA Compra Automática) para `orion/ui`.
3. Extrair **loaders e mapeamentos** de CSV/XLSX para `orion/data`.
4. Extrair **Tiny v3** para `orion/tiny`.
5. Migrar **OC (export + conferência)** para `orion/oc`.
6. Remover gradualmente do `reposicao_facil.py`, mantendo import das novas funções.

## Benefícios
- Manutenção mais simples.
- Alterações localizadas (menos risco).
- Performance do chat (arquivos menores).
