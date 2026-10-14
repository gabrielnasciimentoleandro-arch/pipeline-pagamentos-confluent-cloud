#!/usr/bin/env bash
# =============================================================================
# 04_processamento.sh — CAMADA 4: tópico de alertas + tabelas e job do Flink SQL
# =============================================================================
# Cria, nesta ordem:
#   - tópico alertas.fraude
#   - tabelas Flink: transacoes, contas, alertas_fraude
#   - job contínuo: alerta-fraude-3em60s (3+ transações do mesmo cartão em 60s)
#
# Se preferir criar as tabelas clicando (recomendado na primeira vez), use
# Flink SQL -> Create table -> From topic e cole o DDL gerado nos arquivos de
# flink/. Este script assume o caminho 100% CLI.
# =============================================================================
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p evidencias
EV="evidencias/04-processamento.txt"

if [ -f .env ]; then set -a; . ./.env; set +a; fi
: "${ENV_ID:?defina ENV_ID no .env (scripts/01_fundacao.sh)}"
: "${CLUSTER_ID:?defina CLUSTER_ID no .env}"
: "${COMPUTE_POOL_ID:?defina COMPUTE_POOL_ID no .env}"

criar_statement() { # criar_statement NOME ARQUIVO
  local nome="$1" arquivo="$2"
  echo "-- criando statement: $nome ($arquivo)" | tee -a "$EV"
  confluent flink statement create "$nome" \
    --sql "$(cat "$arquivo")" \
    --compute-pool "$COMPUTE_POOL_ID" \
    --environment "$ENV_ID" \
    --database "$CLUSTER_ID" \
    --wait 2>&1 | tee -a "$EV"
}

{
  echo "################################################################"
  echo "# CAMADA 4 - PROCESSAMENTO (FLINK SQL)  -  $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "################################################################"
} | tee -a "$EV"

echo "== 1/5 Tópico de destino: alertas.fraude ==" | tee -a "$EV"
confluent kafka topic create alertas.fraude --partitions 6 \
  --cluster "$CLUSTER_ID" --environment "$ENV_ID" 2>&1 | tee -a "$EV" || true
confluent kafka topic describe alertas.fraude --cluster "$CLUSTER_ID" --environment "$ENV_ID" | tee -a "$EV"

echo "== 2/5 Tabela de origem (transacoes) ==" | tee -a "$EV"
criar_statement ddl-transacoes flink/01_tabela_transacoes.sql

echo "== 3/5 Tabela de dimensão (contas) ==" | tee -a "$EV"
criar_statement ddl-contas flink/02_tabela_contas.sql

echo "== 4/5 Tabela de destino (alertas_fraude) ==" | tee -a "$EV"
criar_statement ddl-alertas flink/03_tabela_alertas.sql

echo "== 5/5 Job de detecção de fraude ==" | tee -a "$EV"
criar_statement alerta-fraude-3em60s flink/04_alerta_fraude.sql

echo "== Status do job ==" | tee -a "$EV"
confluent flink statement describe alerta-fraude-3em60s --environment "$ENV_ID" | tee -a "$EV"

cat <<'FIM'

------------------------------------------------------------------
CAMADA 4 CONCLUÍDA (evidência em evidencias/04-processamento.txt)

Leitura da evidência:
  - as 3 tabelas foram criadas (DDL) e o job ficou RUNNING
  - quem escreve o alerta é o Flink; o tópico alertas.fraude é o contrato de saída

Próximo passo (dispara a fraude e coleta a prova):
  bash scripts/05_operacao.sh
------------------------------------------------------------------
FIM
