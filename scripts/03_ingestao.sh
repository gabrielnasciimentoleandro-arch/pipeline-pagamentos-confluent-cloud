#!/usr/bin/env bash
# =============================================================================
# 03_ingestao.sh — CAMADA 3: liga o conector CDC do Postgres para o Kafka
# =============================================================================
# Antes de rodar:
#   1. sql/01_schema.sql e sql/02_contas.sql aplicados no Postgres de origem
#   2. conector/postgres-cdc.local.json preenchido (cópia do .json versionado)
#   3. .env com ENV_ID e CLUSTER_ID (gerados por scripts/01_fundacao.sh)
# =============================================================================
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p evidencias
EV="evidencias/03-ingestao.txt"
CONFIG="conector/postgres-cdc.local.json"

if [ ! -f .env ]; then echo "Falta o .env — rode antes scripts/01_fundacao.sh"; exit 1; fi
set -a; . ./.env; set +a
: "${ENV_ID:?defina ENV_ID no .env (scripts/01_fundacao.sh)}"
: "${CLUSTER_ID:?defina CLUSTER_ID no .env}"

if [ ! -f "$CONFIG" ]; then
  cat <<'FIM'
Falta o arquivo do conector com seus valores.

  cp conector/postgres-cdc.json conector/postgres-cdc.local.json
  # edite o .local.json (host, usuário, senha do Postgres e API key do sa-produtor)
  chmod 600 conector/postgres-cdc.local.json
  # e rode este script de novo

Atenção: o .local.json está no .gitignore — nenhum segredo vai para o repositório.
FIM
  exit 1
fi

{
  echo "################################################################"
  echo "# CAMADA 3 - INGESTAO (CDC)  -  $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "################################################################"
} | tee -a "$EV"

echo "== 1/4 Criando o conector ==" | tee -a "$EV"
CONNECTOR_ID=$(confluent connect cluster create \
    --config-file "$CONFIG" \
    --cluster "$CLUSTER_ID" \
    --environment "$ENV_ID" \
    -o json | jq -r '.id // .connector.id')

# o ID do conector não é segredo: pode ir para a evidência
echo "CONNECTOR_ID=$CONNECTOR_ID" | tee -a "$EV"

echo "== 2/4 Aguardando o conector ficar RUNNING (snapshot inicial) ==" | tee -a "$EV"
for _ in $(seq 1 40); do
  ESTADO=$(confluent connect cluster describe "$CONNECTOR_ID" --cluster "$CLUSTER_ID" --environment "$ENV_ID" -o json \
           | jq -r '.status.connector.state // .status.state // "DESCONHECIDO"')
  echo "  estado: $ESTADO"
  [ "$ESTADO" = "RUNNING" ] && break
  sleep 15
done

echo "== 3/4 Tópicos que o conector criou ==" | tee -a "$EV"
confluent kafka topic list --cluster "$CLUSTER_ID" --environment "$ENV_ID" | tee -a "$EV"

echo "== 4/4 Uma mensagem real de CDC (a prova da ingestão) ==" | tee -a "$EV"
echo "--- pgcdc.public.transacoes (primeiros bytes do evento Debezium) ---" | tee -a "$EV"
# O consume roda com o seu login do CLI (papel de administrador). O consumidor de
# verdade (identidade sa-leitor, ACL restrita) é o consumidor/consumir_alertas.py.
confluent kafka topic consume pgcdc.public.transacoes \
  --cluster "$CLUSTER_ID" --environment "$ENV_ID" --max-messages 1 --from-beginning \
  | head -c 1200 | tee -a "$EV" || true

cat <<'FIM'

------------------------------------------------------------------
CAMADA 3 CONCLUÍDA (evidência em evidencias/03-ingestao.txt)

Se o conector estiver RUNNING e a mensagem de CDC apareceu, a ingestão está viva.

Agora teste o UPDATE e o DELETE chegando como evento (o desafio pede isso):

  psql "$DATABASE_URL" -c "UPDATE public.transacoes SET status = 'ESTORNADA' WHERE id = 1;"
  psql "$DATABASE_URL" -c "DELETE FROM public.transacoes WHERE id = 2;"

  confluent kafka topic consume pgcdc.public.transacoes --max-messages 2 --from-beginning | tail -c 1500

Copie a saída para evidencias/03-ingestao.txt.

Próximo passo: camada 2 (contrato) e camada 4 (processamento):
  bash scripts/02_contrato.sh
  bash scripts/04_processamento.sh
------------------------------------------------------------------
FIM
