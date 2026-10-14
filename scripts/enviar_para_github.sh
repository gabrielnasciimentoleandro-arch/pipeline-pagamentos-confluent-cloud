#!/usr/bin/env bash
# =============================================================================
# enviar_para_github.sh — publica ESTE projeto no seu repositório do GitHub
# =============================================================================
# Uso:
#   bash scripts/enviar_para_github.sh https://github.com/SEU-USUARIO/pipeline-pagamentos-confluent-cloud.git
#
# O que ele faz, em ordem:
#   1. checa se algum segredo escapou (chama o 06_checar_segredos.sh)
#   2. confere/assina o commit com o SEU nome e e-mail
#   3. aponta o remote "origin" para o seu repositório
#   4. faz o push da branch main
#
# Pré-requisito: crie ANTES, no GitHub, um repositório VAZIO e PÚBLICO com o
# mesmo nome (sem README, sem .gitignore). Depois rode este script.
# =============================================================================
set -euo pipefail
cd "$(dirname "$0")/.."

ORIGEM="${1:-}"
if [ -z "$ORIGEM" ]; then
  cat <<'FIM'
Uso: bash scripts/enviar_para_github.sh <URL_DO_SEU_REPOSITORIO>

Ex.: bash scripts/enviar_para_github.sh \
       https://github.com/joana-silva/pipeline-pagamentos-confluent-cloud.git
FIM
  exit 1
fi

echo "== 1/4 Checando segredos antes de publicar =="
bash scripts/06_checar_segredos.sh || { echo "Corrija os alertas acima antes de publicar."; exit 1; }

echo
echo "== 2/4 Autor do commit =="
NOME="$(git config user.name || true)"
EMAIL="$(git config user.email || true)"
if [ -z "$NOME" ] || [ "$NOME" = "Seu Nome" ] || [ -z "$EMAIL" ] || [[ "$EMAIL" == *exemplo.com* ]]; then
  read -r -p "  Seu nome (como aparece no GitHub): " NOME
  read -r -p "  Seu e-mail (o mesmo da conta GitHub): " EMAIL
  git config user.name  "$NOME"
  git config user.email "$EMAIL"
  # re-assina TODOS os commits locais com o seu nome (o conteúdo não muda)
  git -c user.name="$NOME" -c user.email="$EMAIL" \
      rebase --root --exec 'git commit --amend --no-edit --reset-author' >/dev/null
  echo "  ok: histórico re-assinado ($(git rev-list --count HEAD) commits)"
else
  echo "  já configurado: $NOME <$EMAIL>"
fi

echo
echo "== 3/4 Configurando o remote =="
if git remote get-url origin >/dev/null 2>&1; then
  git remote set-url origin "$ORIGEM"
else
  git remote add origin "$ORIGEM"
fi
git remote -v | sed 's/^/  /'

echo
echo "== 4/4 Enviando =="
git push -u origin main

cat <<'FIM'

------------------------------------------------------------------
Enviado! Abra o repositório no navegador e confira se os arquivos estão lá.

Se o push pedir usuário e senha: o GitHub não aceita mais senha de conta.
Use um destes dois caminhos:
  - gh auth login          (GitHub CLI instalado) — mais simples
  - Personal Access Token  (GitHub -> Settings -> Developer settings ->
                            Personal access tokens -> Fine-grained)
    Aí, quando pedir a senha, cole o TOKEN.

Lembrete de segurança: token de acesso é como senha — não compartilhe e não
cole dentro de nenhum arquivo do repositório.
------------------------------------------------------------------
FIM
