#!/usr/bin/env bash
# =============================================================================
# 01_fundacao.sh — CAMADA 1: ambiente, cluster, Schema Registry, identidades
# =============================================================================
# Cria:
#   - ambiente "pagamentos"
#   - cluster Kafka Basic
#   - identidades separadas: sa-produtor (escreve) e sa-leitor (lê alertas)
#   - ACLs mínimas para cada identidade (cluster Basic não tem RBAC granular)
#   - compute pool do Flink
#   - arquivo .env com os IDs (sem segredos versionados)
#
# Custo: tudo aqui começa a contar faturação assim que é criado.
# Rode scripts/99_destruir.sh no fim do desafio.
# =============================================================================
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p evidencias

CLOUD="${CLOUD:-aws}"
REGION="${REGION:-us-east-1}"   # exija que a região apareça em `confluent flink region list`
ENV_NAME="${ENV_NAME:-pagamentos}"
CLUSTER_NAME="${CLUSTER_NAME:-cluster-pagamentos}"
POOL_NAME="${POOL_NAME:-pool-pagamentos}"
EV="evidencias/01-fundacao.txt"

salvar_env() { # salvar_env CHAVE valor   (grava/atualiza no .env, que é gitignored)
  local k="$1" v="$2"
  touch .env
  if grep -q "^${k}=" .env; then
    sed -i.bak "s|^${k}=.*|${k}=${v}|" .env && rm -f .env.bak
  else
    echo "${k}=${v}" >> .env
  fi
}

{
  echo "################################################################"
  echo "# CAMADA 1 - FUNDACAO  -  $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "################################################################"
} | tee -a "$EV"

echo "== 1/6 Criando ambiente ==" | tee -a "$EV"
ENV_ID=$(confluent environment create "$ENV_NAME" -o json 2>/dev/null | jq -r '.environment_id // .id' 2>/dev/null) || ENV_ID=""
if [ -z "$ENV_ID" ] || [ "$ENV_ID" = "null" ]; then
  ENV_ID=$(confluent environment list -o json | jq -r --arg n "$ENV_NAME" '.[] | select(.name==$n) | .id' | head -1)
  echo "  (ambiente '$ENV_NAME' já existia — reaproveitado)" | tee -a "$EV"
fi
echo "ENV_ID=$ENV_ID" | tee -a "$EV"
salvar_env ENV_ID "$ENV_ID"
confluent environment use "$ENV_ID" >/dev/null

echo "== 2/6 Criando cluster Kafka (Basic) ==" | tee -a "$EV"
CLUSTER_ID=$(confluent kafka cluster create "$CLUSTER_NAME" --cloud "$CLOUD" --region "$REGION" --type basic -o json 2>/dev/null | jq -r '.cluster_id // .id' 2>/dev/null) || CLUSTER_ID=""
if [ -z "$CLUSTER_ID" ] || [ "$CLUSTER_ID" = "null" ]; then
  CLUSTER_ID=$(confluent kafka cluster list --environment "$ENV_ID" -o json | jq -r --arg n "$CLUSTER_NAME" '.[] | select(.name==$n) | .id' | head -1)
  echo "  (cluster '$CLUSTER_NAME' já existia — reaproveitado)" | tee -a "$EV"
fi
salvar_env CLUSTER_ID "$CLUSTER_ID"
confluent kafka cluster use "$CLUSTER_ID" >/dev/null

echo "  aguardando o cluster ficar PROVISIONED..." | tee -a "$EV"
for _ in $(seq 1 60); do
  STATUS=$(confluent kafka cluster describe "$CLUSTER_ID" -o json | jq -r '.status')
  [ "$STATUS" = "PROVISIONED" ] && break
  sleep 15
done
BOOTSTRAP=$(confluent kafka cluster describe "$CLUSTER_ID" -o json | jq -r '.endpoint')
{
  echo "CLUSTER_ID=$CLUSTER_ID"
  echo "STATUS=$STATUS"
  echo "BOOTSTRAP=$BOOTSTRAP"
} | tee -a "$EV"
salvar_env BOOTSTRAP_SERVERS "$BOOTSTRAP"

echo "== 3/6 Schema Registry do ambiente ==" | tee -a "$EV"
SR_JSON=$(confluent schema-registry cluster describe -o json)
SR_ID=$(echo "$SR_JSON" | jq -r '.cluster_id // .id')
SR_URL=$(echo "$SR_JSON" | jq -r '.endpoint')
{ echo "SR_ID=$SR_ID"; echo "SR_URL=$SR_URL"; } | tee -a "$EV"
salvar_env SR_URL "$SR_URL"

echo "== 4/6 Identidades (o coração da camada) ==" | tee -a "$EV"
SA_PROD=$(confluent iam service-account create sa-produtor --description "Escreve no Kafka (conector CDC)" -o json | jq -r '.id // .resource_id')
SA_LEIT=$(confluent iam service-account create sa-leitor  --description "Le os alertas de fraude" -o json | jq -r '.id // .resource_id')
{ echo "SA_PRODUTOR=$SA_PROD"; echo "SA_LEITOR=$SA_LEIT"; } | tee -a "$EV"
salvar_env SA_PRODUTOR "$SA_PROD"
salvar_env SA_LEITOR "$SA_LEIT"

echo "  criando API keys (a chave secreta aparece UMA vez — copie para o .env)..." | tee -a "$EV"
echo "--- API KEY DO PRODUTOR (usada no conector CDC) ---"
confluent api-key create --resource "$CLUSTER_ID" --service-account "$SA_PROD" --description "kafka-produtor-cdc"
echo "--- API KEY DO LEITOR (usada no consumidor) ---"
confluent api-key create --resource "$CLUSTER_ID" --service-account "$SA_LEIT" --description "kafka-leitor-alertas"
echo "--- API KEY DO LEITOR no Schema Registry (usada no consumidor) ---"
confluent api-key create --resource "$SR_ID" --service-account "$SA_LEIT" --description "sr-leitor"

echo "  ACLs mínimas (cluster Basic usa ACL, não RBAC granular):" | tee -a "$EV"
# Produtor: cria tópicos e escreve só nos tópicos do CDC (prefixo pgcdc.public.)
confluent kafka acl create --allow --service-account "$SA_PROD" --operation CREATE  --cluster-scope
confluent kafka acl create --allow --service-account "$SA_PROD" --operation DESCRIBE --cluster-scope
confluent kafka acl create --allow --service-account "$SA_PROD" --operation WRITE  --operation DESCRIBE --topic pgcdc.public. --prefix
# Leitor: lê apenas o tópico de alertas, com o grupo do consumidor
confluent kafka acl create --allow --service-account "$SA_LEIT" --operation DESCRIBE --cluster-scope
confluent kafka acl create --allow --service-account "$SA_LEIT" --operation READ --operation DESCRIBE --topic alertas.fraude
confluent kafka acl create --allow --service-account "$SA_LEIT" --operation READ --consumer-group consumidor-alertas --prefix

confluent kafka acl list --service-account "$SA_PROD" | tee -a "$EV"
confluent kafka acl list --service-account "$SA_LEIT" | tee -a "$EV"

echo "== 5/6 Compute pool do Flink ==" | tee -a "$EV"
POOL_ID=$(confluent flink compute-pool create "$POOL_NAME" --cloud "$CLOUD" --region "$REGION" --max-cfu 10 -o json 2>/dev/null | jq -r '.id // .compute_pool_id' 2>/dev/null) || POOL_ID=""
if [ -z "$POOL_ID" ] || [ "$POOL_ID" = "null" ]; then
  POOL_ID=$(confluent flink compute-pool list --environment "$ENV_ID" -o json | jq -r --arg n "$POOL_NAME" '.[] | select(.name==$n) | .id' | head -1)
  echo "  (compute pool '$POOL_NAME' já existia — reaproveitado)" | tee -a "$EV"
fi
echo "COMPUTE_POOL_ID=$POOL_ID" | tee -a "$EV"
salvar_env COMPUTE_POOL_ID "$POOL_ID"

echo "== 6/6 Resumo ==" | tee -a "$EV"
confluent kafka cluster describe "$CLUSTER_ID" | tee -a "$EV"

cat <<'FIM'

------------------------------------------------------------------
CAMADA 1 CONCLUÍDA (evidência em evidencias/01-fundacao.txt)

Falta um passo manual, de propósito: copiar as chaves secretas para o .env
(eu não gravo segredo em arquivo de evidência).

  1) No .env, preencha:
       KAFKA_API_KEY / KAFKA_API_SECRET          <- chave do LEITOR no Kafka
       SR_API_KEY    / SR_API_SECRET             <- chave do LEITOR no Schema Registry
  2) Guarde também a chave do PRODUTOR para usar no conector (camada 3).
  3) Depois rode: bash scripts/03_ingestao.sh
------------------------------------------------------------------
FIM
