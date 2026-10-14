#!/usr/bin/env bash
# =============================================================================
# 99_destruir.sh — derruba TUDO o que foi criado (parar de gastar)
# =============================================================================
# Ordem inversa da criação. Rode quando a entrega estiver fechada.
# Uso: bash scripts/99_destruir.sh
# =============================================================================
set -uo pipefail
cd "$(dirname "$0")/.."

if [ -f .env ]; then set -a; . ./.env; set +a; fi

echo "Isto vai apagar: statements do Flink, conector, tópicos, compute pool,"
echo "cluster Kafka, ambiente, service accounts e API keys."
read -r -p "Digite DESTRUIR para confirmar: " resposta
[ "$resposta" = "DESTRUIR" ] || { echo "Cancelado."; exit 1; }

# 1. Statements do Flink (matar o job primeiro)
for stmt in alerta-fraude-3em60s ddl-alertas ddl-contas ddl-transacoes; do
  [ -n "${ENV_ID:-}" ] && confluent flink statement delete "$stmt" --environment "$ENV_ID" --force 2>/dev/null \
    && echo "statement removido: $stmt"
done

# 2. Conector CDC
if [ -n "${CONNECTOR_ID:-}" ] && [ -n "${CLUSTER_ID:-}" ]; then
  confluent connect cluster delete "$CONNECTOR_ID" --cluster "$CLUSTER_ID" --environment "$ENV_ID" --force \
    && echo "conector removido"
fi

# 3. Tópicos
for t in alertas.fraude pgcdc.public.transacoes pgcdc.public.contas; do
  [ -n "${CLUSTER_ID:-}" ] && confluent kafka topic delete "$t" --cluster "$CLUSTER_ID" --environment "$ENV_ID" 2>/dev/null \
    && echo "tópico removido: $t"
done

# 4. Compute pool do Flink
[ -n "${COMPUTE_POOL_ID:-}" ] && confluent flink compute-pool delete "$COMPUTE_POOL_ID" 2>/dev/null && echo "pool removido"

# 5. Cluster e ambiente
[ -n "${CLUSTER_ID:-}" ] && confluent kafka cluster delete "$CLUSTER_ID" --environment "$ENV_ID" 2>/dev/null && echo "cluster removido"
[ -n "${ENV_ID:-}" ] && confluent environment delete "$ENV_ID" 2>/dev/null && echo "ambiente removido"

# 6. API keys e service accounts
for sa in "${SA_PRODUTOR:-}" "${SA_LEITOR:-}"; do
  [ -z "$sa" ] && continue
  for key in $(confluent api-key list --service-account "$sa" -o json 2>/dev/null | jq -r '.[]?|.key // empty'); do
    confluent api-key delete "$key" --force 2>/dev/null && echo "api key removida: $key"
  done
  confluent iam service-account delete "$sa" --force 2>/dev/null && echo "service account removido: $sa"
done

# 7. Limpeza no Postgres de origem
cat <<'FIM'

------------------------------------------------------------------
Faltam dois passos no BANCO (não dá para fazer daqui):

  psql "$DATABASE_URL" -c "SELECT pg_drop_replication_slot('dbz_pagamentos');"
  psql "$DATABASE_URL" -c "DROP PUBLICATION IF EXISTS dbz_pagamentos_pub;"

E confira em Billing / Costs se não ficou nada ligado.
------------------------------------------------------------------
FIM
