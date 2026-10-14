#!/usr/bin/env bash
# =============================================================================
# 05_operacao.sh — CAMADA 5: a prova de ponta a ponta + sinais de operação
# =============================================================================
# 1. dispara a fraude no Postgres (3 transações no mesmo cartão em < 60s)
# 2. espera o CDC + janela do Flink fecharem
# 3. consome alertas.fraude com a identidade de LEITURA (sa-leitor)
# 4. coleta sinais de operação: status do job, métricas, lag, custo
# =============================================================================
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p evidencias
EV="evidencias/05-operacao.txt"

if [ -f .env ]; then set -a; . ./.env; set +a; fi
: "${ENV_ID:?defina ENV_ID no .env}"
: "${CLUSTER_ID:?defina CLUSTER_ID no .env}"

run_psql() { if [ -n "${DATABASE_URL:-}" ]; then psql "$DATABASE_URL" "$@"; else psql "$@"; fi; }

{
  echo "################################################################"
  echo "# CAMADA 5 - OPERACAO (execucao de ponta a ponta)  -  $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "################################################################"
} | tee -a "$EV"

echo "== 1/5 Disparando a fraude no Postgres ==" | tee -a "$EV"
run_psql -f sql/03_lote_fraude.sql 2>&1 | tee -a "$EV"
echo "  (3 transações do mesmo cartão inseridas)" | tee -a "$EV"

echo "== 2/5 Aguardando CDC + janela de 60s do Flink fechar (30s) ==" | tee -a "$EV"
sleep 30

echo "== 3/5 Alertas recebidos pelo consumidor (identidade de leitura) ==" | tee -a "$EV"
if python3 -c "from confluent_kafka.schema_registry.json_schema import JSONDeserializer" 2>/dev/null; then
  python3 consumidor/consumir_alertas.py --timeout 25 2>&1 | tee -a "$EV" || true
else
  echo "  (instale as dependências: pip install -r consumidor/requisitos.txt)" | tee -a "$EV"
fi

echo "== 4/5 Saúde do pipeline ==" | tee -a "$EV"
confluent flink statement describe alerta-fraude-3em60s --environment "$ENV_ID" 2>&1 | tee -a "$EV" || true
confluent kafka consumer group describe consumidor-alertas-01 \
  --cluster "$CLUSTER_ID" --environment "$ENV_ID" 2>&1 | tee -a "$EV" || true

echo "== 5/5 O que ainda precisa de olho humano (capture a tela) ==" | tee -a "$EV"
cat <<'FIM' | tee -a "$EV"

  a) Flink SQL -> SELECT * FROM alertas_fraude;
     (a tela com o alerta enriquecido: titular, limite, cidade da conta x cidade da última transação)

  b) Confluent Cloud -> tópico alertas.fraude -> aba Messages
     (o mesmo alerta chegando no tópico)

  c) Flink -> statement alerta-fraude-3em60s -> Metrics
     (records in/out, throughput, watermarks)

  d) Confluent Cloud -> Conector postgres-cdc-pagamentos -> Metrics
     (eventos por segundo e latência do CDC)

  e) Billing / Costs -> custo do período
     (preencha docs/custos.md e cole o resumo aqui)

  f) Postgres de origem -> pg_stat_replication / pg_replication_slots
     psql "$DATABASE_URL" -c "SELECT slot_name, active, confirmed_flush_lsn FROM pg_replication_slots;"
FIM

cat <<'FIM'

------------------------------------------------------------------
CAMADA 5 CONCLUÍDA (evidência parcial em evidencias/05-operacao.txt)

Se o alerta apareceu, o caminho do dado está fechado:
Postgres -> CDC -> Schema Registry -> Kafka -> Flink SQL -> alertas.fraude -> consumidor

Próximo passo: preencher o custo (docs/custos.md), revisar o README com as
evidências e rodar scripts/99_destruir.sh para parar de gastar.
------------------------------------------------------------------
FIM
