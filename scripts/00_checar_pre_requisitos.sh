#!/usr/bin/env bash
# =============================================================================
# 00_checar_pre_requisitos.sh — confere o ambiente antes de gastar dinheiro
# =============================================================================
set -uo pipefail
cd "$(dirname "$0")/.."

echo "== Pré-requisitos do desafio =="
falhas=0

check() { # check "descrição" comando...
  local desc="$1"; shift
  if "$@" >/dev/null 2>&1; then
    echo "  [ok]   $desc"
  else
    echo "  [FALTA] $desc  ->  $*"
    falhas=$((falhas + 1))
  fi
}

check "Confluent CLI instalado"        confluent version
check "jq instalado"                   jq --version
check "psql instalado"                 psql --version
check "python3 instalado"              python3 --version
check "curl instalado"                 curl --version

if command -v confluent >/dev/null 2>&1; then
  echo
  echo "  Versão do CLI: $(confluent version 2>/dev/null | head -1)"
fi

echo
if command -v confluent >/dev/null 2>&1; then
  echo "== Sessão do CLI =="
  if confluent environment list >/dev/null 2>&1; then
    echo "  [ok]   logado no Confluent Cloud"
  else
    echo "  [FALTA] não está logado. Rode: confluent login --save"
    falhas=$((falhas + 1))
  fi

  echo
  echo "== Regiões onde o Flink está disponível (Kafka e Flink precisam bater) =="
  confluent flink region list 2>/dev/null | head -20 || echo "  (não foi possível listar)"
fi

echo
if [ -f .env ]; then
  echo "  [ok]   arquivo .env encontrado (não versione este arquivo)"
else
  echo "  [info] ainda não existe .env — copie de .env.example e preencha"
fi

echo
if [ "$falhas" -gt 0 ]; then
  echo ">> $falhas item(ns) pendente(s). Resolva antes de rodar scripts/01_fundacao.sh"
  exit 1
fi
echo ">> Ambiente pronto. Próximo passo: scripts/01_fundacao.sh"
