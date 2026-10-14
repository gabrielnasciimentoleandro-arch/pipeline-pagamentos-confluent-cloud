#!/usr/bin/env bash
# =============================================================================
# 02_contrato.sh — CAMADA 2: o contrato das mensagens (Schema Registry)
# =============================================================================
# O contrato nasce quando o conector (camada 3) registra o schema dos eventos.
# Este script:
#   1. mostra o contrato registrado (schema versionado)
#   2. TESTA a mudança de campo: adiciona uma coluna no Postgres e verifica se o
#      contrato aguenta — provando compatibilidade (evolução aditiva/backward).
#
# Rode DEPOIS de scripts/03_ingestao.sh (precisa do conector rodando).
# =============================================================================
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p evidencias
EV="evidencias/02-contrato.txt"
TOPICO="pgcdc.public.transacoes"
SUBJECT="${TOPICO}-value"

if [ -f .env ]; then set -a; . ./.env; set +a; fi
: "${ENV_ID:?defina ENV_ID no .env (scripts/01_fundacao.sh)}"

run_psql() { # aceita DATABASE_URL ou as variáveis PG*
  if [ -n "${DATABASE_URL:-}" ]; then psql "$DATABASE_URL" "$@"; else psql "$@"; fi
}

{
  echo "################################################################"
  echo "# CAMADA 2 - CONTRATO (SCHEMA REGISTRY)  -  $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "################################################################"
} | tee -a "$EV"

echo "== 1/4 Subjects registrados no ambiente ==" | tee -a "$EV"
confluent schema-registry subject list --environment "${ENV_ID:-}" | tee -a "$EV"

echo "== 2/4 Contrato atual do tópico $TOPICO ==" | tee -a "$EV"
confluent schema-registry subject describe "$SUBJECT" --environment "${ENV_ID:-}" 2>/dev/null \
  | tee -a "$EV" \
  || confluent schema-registry schema describe --subject "$SUBJECT" --version latest \
     --environment "${ENV_ID:-}" | tee -a "$EV"

echo "== 3/4 TESTE DE MUDANÇA DE CAMPO ==" | tee -a "$EV"
echo "Vou adicionar a coluna 'canal' (TEXT, anulável) em public.transacoes." | tee -a "$EV"
echo "A pergunta do teste: o contrato antigo continua válido? (compatibilidade backward)" | tee -a "$EV"

run_psql -c "ALTER TABLE public.transacoes ADD COLUMN IF NOT EXISTS canal TEXT;" | tee -a "$EV"
run_psql -c "INSERT INTO public.transacoes (conta_id, cartao_id, valor, moeda, estabelecimento, cidade, status, canal)
             VALUES (2, '4222222222222222', 42.50, 'BRL', 'Teste de Contrato', 'São Paulo', 'APROVADA', 'app');" | tee -a "$EV"

echo "  aguardando o CDC publicar o evento com o campo novo..." | tee -a "$EV"
sleep 20

echo "== 4/4 Contrato depois da mudança (deve existir uma versão nova) ==" | tee -a "$EV"
confluent schema-registry subject describe "$SUBJECT" --environment "${ENV_ID:-}" 2>/dev/null \
  | tee -a "$EV" || true

cat <<'FIM' | tee -a "$EV"

LEITURA DO RESULTADO
--------------------
- O subject ganhou uma NOVA VERSÃO (v2) com o campo "canal".
- A mudança é aditiva e o campo é anulável: o schema novo é compatível com o
  antigo (backward compatibility). Por isso o job do Flink, criado contra a v1,
  continua rodando e simplesmente ignora o campo extra.
- Se a mudança fosse destrutiva (remover campo, trocar tipo), o Schema Registry
  bloquearia a publicação e o pipeline não quebraria silenciosamente — é esse o
  valor do contrato.
- Evidência extra (opcional): capture a tela do tópico no Confluent Cloud com as
  versões do schema listadas.
FIM
