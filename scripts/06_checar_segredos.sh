#!/usr/bin/env bash
# =============================================================================
# 06_checar_segredos.sh — roda ANTES de dar push (evita vazar credencial)
# =============================================================================
# Procura no que seria versionado:
#   1. arquivos de segredo que não deveriam existir aqui (.env, *.local.json)
#   2. chaves no formato da Confluent (ex.: ABCD1234...)
#   3. strings tipo "api_secret = <valor>"
#   4. URLs de banco com usuário:senha embutidos
#
# Uso: bash scripts/06_checar_segredos.sh
# =============================================================================
set -uo pipefail
cd "$(dirname "$0")/.."

problemas=0
ALVO=(--include="*.json" --include="*.py" --include="*.sh" --include="*.sql"
      --include="*.md" --include="*.yml" --include="*.txt" --include="*.example" --include="*.tf")

echo "== 1) Arquivos de segredo presentes no diretório =="
for f in .env conector/*.local.json *.pem *.key terraform.tfstate; do
  [ -e "$f" ] && { echo "  [ATENÇÃO] existe: $f (confirme que está no .gitignore e NUNCA faça commit)"; }
done
echo "  (se apareceu algo acima, está coberto pelo .gitignore? $(grep -q '^\.env$' .gitignore && echo 'sim para .env') )"

echo "== 2) Chaves no formato da Confluent (API keys / secrets) =="
if grep -rInE "\b[A-Z]{4,6}[A-Z0-9]{10,}[0-9A-Z]{4,}\b" "${ALVO[@]}" . 2>/dev/null | grep -vE "sua_|SEU_|<[A-Z_]+>|API_KEY_DO|placeholder"; then
  echo "  [ATENÇÃO] possível credencial acima"; problemas=$((problemas+1))
else
  echo "  ok"
fi

echo "== 3) Atribuições suspeitas (secret/password com valor) =="
if grep -rInE "(api[_-]?secret|api[_-]?key|password|senha)[\"']?\s*[:=]\s*[\"']?[A-Za-z0-9/+_-]{16,}" "${ALVO[@]}" . 2>/dev/null \
   | grep -vE "<[^>]+>|sua_|SEU_|placeholder"; then
  echo "  [ATENÇÃO] valor atribuído acima"; problemas=$((problemas+1))
else
  echo "  ok"
fi

echo "== 4) URLs de banco com credencial embutida =="
if grep -rInE "postgres(ql)?://[^\"'[:space:]]+:[^\"'@[:space:]]+@" "${ALVO[@]}" . 2>/dev/null | grep -v "usuario:senha"; then
  echo "  [ATENÇÃO] URL com senha real acima"; problemas=$((problemas+1))
else
  echo "  ok"
fi

echo
if [ "$problemas" -gt 0 ]; then
  echo ">> $problemas alerta(s). Resolva antes do push."
  exit 1
fi
echo ">> Nenhum segredo encontrado no que seria versionado. Pode dar push."
