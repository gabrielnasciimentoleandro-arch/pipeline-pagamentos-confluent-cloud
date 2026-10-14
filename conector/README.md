# Camada 3 — Ingestão (conector CDC)

O arquivo `postgres-cdc.json` é o contrato de configuração do conector
**PostgreSQL CDC Source V2 (Debezium)** do Confluent Cloud.

## Como criar o conector

```bash
# 1. Preencha o arquivo com seus valores (sem commitar segredos!)
cp conector/postgres-cdc.json /tmp/postgres-cdc.json
# edite /tmp/postgres-cdc.json

# 2. Crie o conector no cluster
confluent connect cluster create \
  --config-file /tmp/postgres-cdc.json \
  --cluster "$CLUSTER_ID" \
  --environment "$ENV_ID"

# 3. Evidência da camada 3
confluent connect cluster list --cluster "$CLUSTER_ID" --environment "$ENV_ID" \
  | tee evidencias/03-ingestao.txt
```

> A chave de API usada aqui é a do **service account produtor** (`sa-produtor`).
> Ela é a "identidade de quem escreve" no pipeline — o tópico `alertas.fraude`
> continua inacessível para ela, e vice-versa.

## Por que cada propriedade

| Propriedade | Valor | Por quê |
|---|---|---|
| `connector.class` | `PostgresCdcSourceV2` | Versão gerenciada e atual do conector Debezium para Postgres no Confluent Cloud. |
| `kafka.auth.mode` + `kafka.api.key/secret` | API key do `sa-produtor` | O conector escreve no Kafka **com a identidade que você fornece** — é isso que permite separar quem escreve de quem lê. |
| `database.*` | host/porta/usuário/senha/dbname | Conexão com o Postgres de origem. |
| `table.include.list` | `public.transacoes,public.contas` | Capturamos só o que interessa (fato + dimensão), em vez do banco inteiro. |
| `topic.prefix` | `pgcdc` | Namespace dos tópicos gerados pelo Debezium: `<prefixo>.<schema>.<tabela>` → `pgcdc.public.transacoes`. |
| `slot.name` | `dbz_pagamentos` | Slot de replicação lógica. Um slot por conector, nome estável. |
| `publication.name` | `dbz_pagamentos_pub` | Publicação pgoutput criada em `sql/01_schema.sql`. |
| `publication.autocreate.mode` | `filtered` | Se a publicação não existir, criar só para as tabelas do filtro (nunca `all_tables`). |
| `snapshot.mode` | `initial` | Faz o snapshot do estado atual e, depois, passa a seguir o WAL ao vivo. |
| `output.data.format` | `JSON_SR` | Registra o schema no Schema Registry (JSON Schema) — é o "contrato" da camada 2. Lembre: o envelope Debezium (`before/after/op`) é preservado. |
| `output.key.format` | `JSON_SR` | A chave também é contrato: `id` da linha. |
| `decimal.handling.mode` | `double` | `NUMERIC` chega como `double` em vez de decimal binário — simplifica o DDL do Flink e evita erro de precisão na janela. Se o conector recusar a propriedade, remova-a (o Flink passa a enxergar `DECIMAL`). |
| `tasks.max` | `1` | Um conector CDC = um slot = uma task. Aumentar não traz paralelismo ao Postgres. |

## Opções que ficaram de fora (e por quê)

- **`after.state.only`**: descartaria o `before`, perdendo a prova de UPDATE/DELETE — o desafio pede para observar o update e o delete chegando como evento.
- **`heartbeat.interval.ms`**: útil para manter o slot vivo em bases ociosas; ficou fora porque cria um tópico extra (`__debezium-heartbeat.pgcdc`) que sairia fora da ACL do produtor. Em produção, inclua e ajuste a ACL.
- **`errors.tolerance` / DLQ**: evolução sugerida no README — em vez de derrubar o conector, mandar o evento com defeito para uma fila separada.

## Erros conhecidos

| Mensagem | Causa provável | Correção |
|---|---|---|
| `publication ... does not exist` | Publicação não criada / permissão | Rode `sql/01_schema.sql` no banco. |
| `replication slot ... already exists` | Slot órfão de tentativa anterior | `SELECT pg_drop_replication_slot('dbz_pagamentos');` com o conector parado. |
| `timeout / connection refused` | Postgres não aceita conexão da internet | Verifique `listen_addresses`, `pg_hba.conf` e firewall (o Confluent Cloud precisa alcançar o host). |
| `wal_level is not logical` | Servidor sem replicação lógica | Ajuste `wal_level = logical` e reinicie o Postgres. |
