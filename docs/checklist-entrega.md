# Checklist de entrega (o que a avaliação olha)

> Princípio do desafio: **evidência é o que separa a entrega que se prova da que só se descreve.**
> Rode o comando, copie a saída, cole no README/evidências. Sem evidência = camada não comprovada.

## Repositório

- [ ] Público no GitHub, na **sua** conta
- [ ] Nome em **minúsculas e sem acento** (ex.: `pipeline-pagamentos-confluent-cloud`)
- [ ] O link enviado é o do **repositório** (não de um arquivo dentro dele)
- [ ] Sem `.env`, sem chave de API, sem senha de banco; `.env.example` no lugar do `.env`
- [ ] `.gitignore` cobrindo `.env`, `conector/*.local.json` e `terraform.tfstate`
- [ ] `bash scripts/06_checar_segredos.sh` rodado e sem alertas antes do push

## Camadas (cada uma com evidência)

- [ ] **1. Fundação** — cluster + Schema Registry + duas identidades + ACLs
      → saída de `confluent kafka cluster describe` e `confluent kafka acl list`
- [ ] **2. Contrato** — schema registrado e teste de mudança de campo
      → `confluent schema-registry subject list` + versões antes/depois
- [ ] **3. Ingestão** — conector `RUNNING` e um evento Debezium real
      → `confluent connect cluster describe` + `confluent kafka topic consume` (com `op: c/u/d`)
- [ ] **4. Processamento** — tabelas + job do Flink
      → status `RUNNING` + `SELECT * FROM alertas_fraude;`
- [ ] **5. Operação** — a prova de ponta a ponta
      → alerta recebido + métricas (Flink, conector) + custo (Billing)

## README (o avaliador lê por aqui)

- [ ] O que **cada camada** faz, com evidência de que rodou
- [ ] **Por que** cada peça do caminho (CDC x batch, Flink SQL x consumidor, janela de 60s…)
- [ ] Como **subir do zero** (comandos na ordem)
- [ ] Como **derrubar tudo** (parar de gastar)
- [ ] **Quanto custou** rodar (números reais)
- [ ] Nenhum `<sua evidência aqui>` sobrando

## Teste final (5 minutos, vale muito)

- [ ] Clone o repositório em uma pasta nova e siga **só o README**:
      dá para subir? Se você travou em algum passo, quem avaliar também trava.
- [ ] Abra cada arquivo de evidência e confirme que é saída **real** (não digitada de memória/estimada)
- [ ] Rode `bash scripts/99_destruir.sh` e confirme em *Billing* que parou de contar
