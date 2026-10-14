# Pipeline de Pagamentos em Tempo Real — Postgres → CDC → Confluent Cloud → Flink SQL

> **Desafio de projeto DIO — "Pipeline Real-Time End-to-End com Confluent Cloud".**
> Os dados nascem em um Postgres, entram no Confluent Cloud por **CDC**, passam pelo
> **Schema Registry** (contrato), são enriquecidos e agregados no **Flink SQL** e saem
> como **alerta de fraude** em um tópico pronto para consumo.
>
> Regra de negócio do alerta: **3+ transações do mesmo cartão dentro de 60 segundos**.

Repositório: **`pipeline-pagamentos-confluent-cloud`** (público, nome em minúsculas e sem acento).

---

## O que este pipeline faz

Um pipeline de pagamentos de ponta a ponta, em streaming, com cinco camadas:

1. **Fundação** — ambiente, cluster, Schema Registry e **duas identidades** (quem escreve ≠ quem lê).
2. **Contrato** — o formato das mensagens é registrado no Schema Registry e testado contra uma mudança de campo.
3. **Ingestão** — o conector **CDC (Debezium)** transforma `INSERT`/`UPDATE`/`DELETE` do Postgres em eventos Kafka.
4. **Processamento** — o **Flink SQL** junta transação + conta e aplica a regra da janela de 60 segundos.
5. **Operação** — métricas, lag, checagem de saúde e **custo** do que ficou ligado.

## Arquitetura

```mermaid
flowchart LR
    subgraph ORIGEM["Postgres (origem transacional)"]
        T["public.transacoes"]
        C["public.contas"]
    end

    subgraph CF[Confluent Cloud]
        CDC["Conector CDC\nPostgresCdcSourceV2\n(assina com sa-produtor)"]
        SR[["Schema Registry\ncontrato JSON_SR"]]
        K1[["pgcdc.public.transacoes"]]
        K2[["pgcdc.public.contas"]]
        FL["Flink SQL\ntabelas + job:\njanela 60s + join com contas"]
        K3[["alertas.fraude\n(upsert por cartão+janela)"]]
    end

    CON["Consumidor Python\n(assina com sa-leitor)"]

    T --> CDC
    C --> CDC
    CDC -->|antes/depois + op| K1
    CDC --> K2
    K1 --> FL
    K2 --> FL
    FL --> K3
    K3 --> CON
    SR -.->|"registra e valida o schema de cada evento"| K1
    SR -.-> K3
```

Resumo em uma linha:

```
Postgres → CDC (Debezium) → Schema Registry → Kafka → Flink SQL (janela 60s) → alertas.fraude → consumidor
```

## O caminho do dado, camada por camada

| # | Camada | O que faz | Onde está no repositório |
|---|---|---|---|
| 1 | Fundação | Cria ambiente, cluster Basic, Schema Registry, compute pool e duas identidades com ACL mínima | `scripts/01_fundacao.sh`, `.env.example` |
| 2 | Contrato | Valida que o schema dos eventos evolui sem quebrar quem consome | `scripts/02_contrato.sh` |
| 3 | Ingestão | Conector CDC `PostgresCdcSourceV2` publicando `pgcdc.public.transacoes` e `pgcdc.public.contas` | `conector/postgres-cdc.json`, `scripts/03_ingestao.sh` |
| 4 | Processamento | Tabelas + job Flink SQL: janela de 60s por tempo de evento, contagem por cartão, enriquecimento com titular/limite/cidade | `flink/*.sql`, `scripts/04_processamento.sh` |
| 5 | Operação | Rajada de fraude, leitura dos alertas, status do job, métricas e custo | `scripts/05_operacao.sh`, `docs/custos.md` |

## Modelo de dados e regra de fraude

**Fonte (Postgres):**

| Tabela | Papel | Campos principais |
|---|---|---|
| `public.contas` | dimensão (enriquece o alerta) | `id, nome, limite, cidade` |
| `public.transacoes` | fato (vira evento) | `id, conta_id, cartao_id, valor, moeda, estabelecimento, cidade, status, criado_em` |

Cartão fixo por conta (facilita reproduzir a fraude): conta 1 → `****1111`, conta 2 → `****2222`, e assim por diante.

**Regra:** contar transações do mesmo `cartao_id` em janelas de **60 segundos** por **tempo de evento** (`criado_em`), e emitir alerta quando `COUNT(*) >= 3`, enriquecido com os dados da conta.

## Estrutura do repositório

```
.
├── README.md                     <- esta entrega (evidências por camada)
├── .env.example                  <- modelo de variáveis (nenhum segredo real aqui)
├── .gitignore                    <- .env, .local.json, tfstate, caches
├── docker-compose.yml            <- Postgres local para ensaiar o CDC (opcional)
├── sql/
│   ├── 01_schema.sql             <- tabelas, REPLICA IDENTITY FULL, publicação
│   ├── 02_contas.sql             <- dados sintéticos das contas
│   └── 03_lote_fraude.sql        <- 3 transações do mesmo cartão em <60s
├── scripts/
│   ├── 00_checar_pre_requisitos.sh
│   ├── 01_fundacao.sh            <- camada 1
│   ├── 02_contrato.sh            <- camada 2
│   ├── 03_ingestao.sh            <- camada 3
│   ├── 04_processamento.sh       <- camada 4
│   ├── 05_operacao.sh            <- camada 5
│   ├── 06_checar_segredos.sh     <- rodar antes de cada push
│   ├── enviar_para_github.sh     <- publica o projeto no seu GitHub (1 comando)
│   ├── 99_destruir.sh            <- derruba tudo (parar de gastar)
│   ├── gerador_transacoes.py     <- dados sintéticos em tempo real
│   └── requisitos.txt
├── conector/
│   ├── postgres-cdc.json         <- configuração do conector (versionada, sem segredos)
│   └── README.md                 <- por que cada propriedade do conector
├── flink/
│   ├── 01_tabela_transacoes.sql
│   ├── 02_tabela_contas.sql
│   ├── 03_tabela_alertas.sql
│   ├── 04_alerta_fraude.sql      <- o job da regra de fraude
│   └── README.md
├── consumidor/
│   ├── consumir_alertas.py       <- lê alertas.fraude com a identidade de leitura
│   └── requisitos.txt
├── docs/
│   ├── custos.md                 <- como medir e onde achar o custo
│   └── checklist-entrega.md      <- checklist do desafio
└── evidencias/                   <- saídas reais de execução (a prova)
```

## Pré-requisitos

| Ferramenta | Para quê | Como obter |
|---|---|---|
| Conta no Confluent Cloud | cluster, Schema Registry, Flink | <https://confluent.cloud/signup> (procure cupom de crédito de teste) |
| Confluent CLI ≥ 3.x | criar tudo por linha de comando | <https://docs.confluent.io/confluent-cli/current/install.html> |
| Postgres 13+ acessível pela internet | fonte do CDC | Postgres do lab, ou gerenciado (Neon, Supabase, RDS…). O conector roda na nuvem da Confluent: ele precisa alcançar o banco |
| `psql`, `jq`, `python3`, `curl` | scripts e evidências | seu sistema |
| `pip install -r scripts/requisitos.txt -r consumidor/requisitos.txt` | gerador e consumidor | — |

Verificação automática: `bash scripts/00_checar_pre_requisitos.sh`

> **Escolhendo a região:** Kafka e Flink precisam estar na mesma nuvem e região.
> Rode `confluent flink region list` e escolha uma região que apareça lá
> (ex.: `aws us-east-1`). O script usa `REGION=us-east-1` por padrão — troque com
> `REGION=sa-east-1 bash scripts/01_fundacao.sh` se sua região tiver Flink.

---

## Execução do zero (runbook)

### Preparar a fonte (Postgres)

```bash
# aponte para o SEU Postgres (o .env é só para isso)
export DATABASE_URL="postgresql://usuario:senha@host:5432/banco"

psql "$DATABASE_URL" -f sql/01_schema.sql     # tabelas + publicação + replica identity
psql "$DATABASE_URL" -f sql/02_contas.sql     # 5 contas sintéticas
```

Quer tentar localmente antes (só para ensaiar, o conector não alcança o seu `localhost`)?

```bash
docker compose up -d postgres
export PGHOST=localhost PGPORT=5432 PGDATABASE=pagamentos PGUSER=postgres PGPASSWORD=postgres
```

### Camada 1 — Fundação

```bash
confluent login --save
bash scripts/01_fundacao.sh          # cria ambiente, cluster, SR, 2 identidades, ACLs, compute pool
# copie as chaves secretas para o .env conforme o script pede no final
```

O que essa camada entrega e por quê:

- **Duas identidades:** `sa-produtor` escreve no Kafka (usada pelo conector CDC) e `sa-leitor` só lê `alertas.fraude`. Cluster **Basic** não tem papéis RBAC granulares — então a granularidade é feita com **ACLs** (write apenas no prefixo `pgcdc.public.`, read apenas em `alertas.fraude`).
- Os IDs (não-secretos) ficam registrados em `.env` e em `evidencias/01-fundacao.txt`. **Nenhuma chave secreta é versionada.**

📌 **Evidência da camada 1** — rode `confluent kafka cluster describe "$CLUSTER_ID"` e
`confluent kafka acl list --service-account "$SA_PRODUTOR"` e cole a saída:

```text
<sua evidência aqui>
```

### Camada 3 — Ingestão (CDC)

```bash
cp conector/postgres-cdc.json conector/postgres-cdc.local.json
# edite o .local.json: host/usuário/senha do Postgres e a API key do sa-produtor
chmod 600 conector/postgres-cdc.local.json

bash scripts/03_ingestao.sh          # cria o conector e espera ficar RUNNING
```

Detalhes de cada propriedade do conector: [`conector/README.md`](conector/README.md).

Depois de RUNNING, prove que **UPDATE e DELETE** também viram evento:

```bash
psql "$DATABASE_URL" -c "UPDATE public.transacoes SET status='ESTORNADA' WHERE id=1;"
psql "$DATABASE_URL" -c "DELETE FROM public.transacoes WHERE id=2;"
confluent kafka topic consume pgcdc.public.transacoes --max-messages 2
```

📌 **Evidência da camada 3** — saída do conector `RUNNING` + um evento Debezium real
(repare no `op: c/u/d` e nos blocos `before`/`after`):

```text
<sua evidência aqui>
```

### Camada 2 — Contrato (Schema Registry)

```bash
bash scripts/02_contrato.sh          # lista os subjects e TESTA uma mudança de campo
```

O teste: adicionamos a coluna `canal` em `public.transacoes`. O contrato ganha uma
**nova versão** e o job do Flink, criado contra a v1, continua rodando (evolução
aditiva = compatível). Se a mudança fosse destrutiva, o Schema Registry bloquearia a
publicação — o pipeline não quebra em silêncio.

📌 **Evidência da camada 2** — subjects e as versões do schema antes/depois:

```text
<sua evidência aqui>
```

### Camada 4 — Processamento (Flink SQL)

```bash
bash scripts/04_processamento.sh     # tópico alertas.fraude + 3 tabelas + job
```

Ou, se preferir clicar: *Flink SQL → Create table → From topic* para cada tópico
(a UI gera o DDL com os tipos exatos) e depois crie o job colando
`flink/04_alerta_fraude.sql`. Cole o DDL gerado de volta nos arquivos de `flink/`
para manter a configuração versionada.

📌 **Evidência da camada 4** — as 3 tabelas criadas e o job `RUNNING`:

```text
<sua evidência aqui>
```

### Camada 5 — Operação (a prova de ponta a ponta)

```bash
bash scripts/05_operacao.sh          # rajada de fraude + consumo dos alertas + saúde
```

Se aparecer algo assim, o caminho do dado está fechado:

```text
[ALERTA] FRAUDE_3_EM_60S | cartao ****1111 | titular Ana Souza | 3 tx | total R$ 1238.80
         | janela 2026-10-14 18:02:00 -> 18:03:00 | cidade da ultima tx: Miami
```

📌 **Evidência da camada 5** — alerta no tópico/consumidor + métricas:

```text
<sua evidência aqui>
```

---

## Como derrubar tudo

```bash
bash scripts/99_destruir.sh          # pede confirmação "DESTRUIR"
```

Apaga statements do Flink, conector, tópicos, compute pool, cluster, ambiente,
service accounts e API keys, e lembra de soltar o slot/publicação no Postgres.
**Rode isso quando fechar a entrega — é o que faz o custo parar de subir.**

## Quanto custou rodar

> Preencha com os números da sua conta. Passo a passo em [`docs/custos.md`](docs/custos.md).

| Item | Como é cobrado | Uso no desafio | Custo no período |
|---|---|---|---|
| Cluster Kafka (Basic) | por hora | ___ h | US$ ___ |
| Conector CDC (Postgres) | por hora por conector | ___ h | US$ ___ |
| Flink (compute pool) | por CFU-hora | ___ CFU · ___ h | US$ ___ |
| Schema Registry | por hora | ___ h | US$ ___ |
| Armazenamento/rede | por GB | ___ GB | US$ ___ |
| **Total** | | | **US$ ___** |

Como o lugar certo é a própria fatura, a evidência é a captura de
*Billing → Costs* do período. Para reduzir: rodar as camadas em sequência,
não deixar o compute pool ligado sem uso e destruir tudo no fim.

## Decisões de arquitetura (por que cada peça)

- **Por que CDC e não batch?** O alerta precisa sair enquanto a fraude acontece. Batch descobriria a rajada no dia seguinte — e o custo de varrer a tabela inteira de hora em hora é maior, na maioria dos casos. O CDC lê o log de replicação do Postgres (WAL) e entrega cada alteração como evento, sem consultar a tabela.
- **Por que Flink SQL e não um consumidor próprio?** A regra é uma janela temporal com agregação e join — exatamente o que SQL de streaming resolve com estado gerenciado, checkpoint e escalabilidade. Um consumidor Python exigiria implementar janela, estado e deduplicação à mão.
- **Por que a janela é de 60 segundos?** É o equilíbrio pedido no desafio: tempo curto o suficiente para reagir (bloquear cartão, mandar SMS) e longo o suficiente para não acusar o uso normal de quem compra duas coisas seguidas. Como o parâmetro está no SQL, mudar para 30s/120s é uma linha.
- **Por que tempo de evento (`criado_em`) com watermark de 5s?** A fraude deve ser medida pelo momento da compra, não pelo momento em que o evento chegou no Kafka. O watermark de 5 segundos dá folga para eventos fora de ordem sem atrasar o alerta.
- **Por que o alerta é um *upsert* por `(cartão, janela)`?** A contagem dentro de uma janela muda a cada evento novo. Em vez de gerar um alerta duplicado por evento, o tópico guarda o **estado atual** do alerta daquela janela — o consumidor sempre lê o número mais recente.
- **Por que `TUMBLE` (janela fixa) e não `HOP` (deslizante)?** Janela fixa é a leitura literal da regra e produz um alerta por janela. O trade-off: uma rajada que começa em 00:59 e termina em 01:01 pode se dividir entre duas janelas. A alternativa `HOP` (alternando em `flink/04_alerta_fraude.sql`) cobre a fronteira ao custo de alertas repetidos.
- **Por que duas identidades?** "Uma identidade para quem escreve e outra para quem lê" é requisito do desafio e boa prática: o produtor não lê alertas, o leitor não escreve nada, e o dano possível de uma chave vazada fica limitado ao seu escopo.
- **Por que `JSON_SR` (JSON Schema no Registry) e não Avro?** É legível em qualquer tópico/CLI, o que ajuda na hora de mostrar evidência, e mantém o envelope Debezium (`before/after/op`) reconhecido pelo Flink como `json-debezium-registry`. Avro seria mais compacto — fica como evolução.
- **Por que `REPLICA IDENTITY FULL`?** Para que `UPDATE` e `DELETE` cheguem com o registro **completo** no bloco `before`. Sem isso, o DELETE carregaria só a chave primária. O custo é mais WAL — aceitável neste cenário e consciente.
- **Por que um Postgres com dados sintéticos?** O domínio (pagamentos) é só o cenário: o desafio avalia o **caminho do dado**. O gerador (`scripts/gerador_transacoes.py`) produz tráfego realista sem depender de dados sensíveis.

## Segurança e segredos

- `.env` e `conector/*.local.json` estão no `.gitignore` — o que vai para o repositório é `.env.example`.
- Cada chave pertence a uma identidade (`sa-produtor`, `sa-leitor`) e tem ACL mínima.
- Nada de host/usuário/senha do banco versionados: o arquivo versionado do conector usa `<placeholders>`.
- As evidências não incluem secrets: os scripts gravam apenas IDs e saídas de status.
- **Antes de cada push**, rode o verificador: `bash scripts/06_checar_segredos.sh`
  (procura `.env`, chaves no formato da Confluent, senhas atribuídas e URLs com credencial).

## Troubleshooting

| Sintoma | Causa provável | Correção |
|---|---|---|
| Conector `FAILED` com `wal_level is not logical` | Postgres sem replicação lógica | `ALTER SYSTEM SET wal_level='logical'; ` + restart (ou `rds.logical_replication=1` na RDS) |
| Conector não conecta no banco | O Confluent roda na nuvem e precisa alcançar seu Postgres | Use banco gerenciado acessível pela internet (Neon, Supabase, RDS público) — `localhost` não funciona |
| `replication slot "dbz_pagamentos" already exists` | Slot órfão de tentativa anterior | `SELECT pg_drop_replication_slot('dbz_pagamentos');` com o conector parado |
| Erro ao criar compute pool | Região sem Flink | `confluent flink region list` e recrie cluster+pool na mesma região |
| `value.format ... json-debezium-registry` não aceito | Schema do tópico ainda não registrado | Confirme `output.data.format = JSON_SR` no conector e `confluent schema-registry subject list` |
| Tipos diferentes no DDL do Flink | O tipo de `criado_em` depende do formato gerado | Use o DDL que a UI gera (`Create table from topic`) e atualize `flink/01_...sql` |
| Alertas não aparecem | Rajada caiu fora da janela ou o watermark ainda não passou | Rode `sql/03_lote_fraude.sql` de novo e aguarde ~30s |
| Consumidor sem permissão | ACL não cobre o tópico/grupo | Revise `confluent kafka acl list --service-account "$SA_LEITOR"` |

## Ideias para evoluir

- Trocar o domínio: telemetria de IoT ou personalização em tempo real, sem mexer na arquitetura.
- **Segunda regra de fraude** (outra janela, outro critério) rodando em paralelo.
- **Fila de eventos com defeito** (`errors.tolerance` + DLQ) em vez de derrubar o conector.
- Materializar os tópicos como tabelas **Iceberg/Delta com Tableflow** para consultas históricas.
- Enriquecer o stream com **modelo de IA** que pontua o risco de cada transação.

## Checklist final de entrega

- [ ] Repositório público, nome em minúsculas e sem acento
- [ ] O link enviado é o do repositório (não o de um arquivo dentro dele)
- [ ] Cada camada tem evidência real (print, saída de comando ou o próprio evento)
- [ ] Nenhum `<sua evidência aqui>` sobrando no README
- [ ] Nenhuma senha ou chave versionada; `.env.example` no lugar do `.env`
- [ ] Outra pessoa consegue subir o pipeline seguindo apenas este README
- [ ] README responde: o que cada camada faz, por que cada peça, como subir do zero, como derrubar, quanto custou

Detalhamento em [`docs/checklist-entrega.md`](docs/checklist-entrega.md).

---

**Autor:** _<seu nome>_ · Desafio de projeto DIO — Pipeline Real-Time End-to-End com Confluent Cloud.
