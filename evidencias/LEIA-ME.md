# Evidências

Esta pasta guarda a **saída real** de cada camada. Os scripts já escrevem aqui:

| Arquivo | Gerado por | Camada |
|---|---|---|
| `01-fundacao.txt` | `scripts/01_fundacao.sh` | 1 — Fundação |
| `02-contrato.txt` | `scripts/02_contrato.sh` | 2 — Contrato |
| `03-ingestao.txt` | `scripts/03_ingestao.sh` | 3 — Ingestão |
| `04-processamento.txt` | `scripts/04_processamento.sh` | 4 — Processamento |
| `05-operacao.txt` | `scripts/05_operacao.sh` | 5 — Operação |
| `05-custos.json` | comando de billing (ver `docs/custos.md`) | 5 — Operação |

## Regras

1. **Não invente evidência.** Se um comando falhou, registre o erro e a correção — isso também é evidência de operação.
2. **Sem segredos aqui.** `API_SECRET`, senha de banco e afins não entram. Os scripts já cuidam disso; nas capturas manuais, confira antes de subir.
3. **Cole também no README** o trecho principal (o avaliador lê o README primeiro).
4. Prints de tela são bem-vindos para métricas e Billing: salve como PNG (ex.: `05-metricas-flink.png`) e referencie no README.
