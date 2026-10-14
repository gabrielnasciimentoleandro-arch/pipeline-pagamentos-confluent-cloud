# Camada 4 — Processamento (Flink SQL)

Ordem de criação (as tabelas precisam existir antes do job):

| Ordem | Arquivo | O que é |
|---|---|---|
| 1 | `01_tabela_transacoes.sql` | Tabela de origem (tópico `pgcdc.public.transacoes`) |
| 2 | `02_tabela_contas.sql` | Tabela de dimensão (tópico `pgcdc.public.contas`) |
| 3 | `03_tabela_alertas.sql` | Tabela de destino (tópico `alertas.fraude`, upsert) |
| 4 | `04_alerta_fraude.sql` | O job: janela de 60s + contagem + enriquecimento |

## Dois caminhos para criar as tabelas

**A. Pela UI (recomendado na primeira vez).**
No Confluent Cloud: *Flink SQL* → **Create table** → **From topic** → escolha o
tópico `pgcdc.public.transacoes`. A UI lê o schema do Schema Registry e gera o
DDL **com os tipos exatos** (inclusive o tipo da coluna `criado_em`).
Copie o DDL gerado para o arquivo correspondente aqui no repositório e faça
commit — assim a configuração fica versionada em código.

**B. Pelo script.**
`bash scripts/03_criar_statement_alerta.sh` executa os quatro arquivos em
sequência via CLI (`confluent flink statement create`).

## Observações que evitam dor de cabeça

- **`value.format = 'json-debezium-registry'`**: o conector foi configurado com
  `output.data.format = JSON_SR`, ou seja, os eventos são JSON *com envelope
  Debezium* registrado no Schema Registry. É esse formato que o Flink reconhece
  para abrir o `before/after/op`. Se o DDL não reconhecer o formato, verifique se
  o Schema Registry está habilitado no ambiente e se o schema do tópico está
  registrado (`confluent schema-registry subject list`).
- **`changelog.mode = 'retract'` nas origens**: necessário para que o Flink
  entenda UPDATE (`-U/+U`) e DELETE (`-D`) vindos do CDC, em vez de tratar tudo
  como INSERT novo.
- **`changelog.mode = 'upsert'` no destino**: a contagem da janela muda conforme
  novos eventos chegam; o sink guarda o **estado** do alerta por
  `(cartao_id, janela_inicio)` em vez de acumular cópias.
- **Watermark de 5s**: dá tempo de os eventos fora de ordem chegarem antes de a
  janela "fechar" e emitir o resultado.
- **Timezone**: `criado_em` vem como `timestamptz`; o Flink usa UTC no
  `DATE_FORMAT`. Se seus horários aparecerem deslocados, ajuste a formatação
  (é a única diferença esperada entre ambientes).
