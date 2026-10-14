# Quanto custou rodar (camada 5 — Operação)

O desafio pede o custo real do que você subiu. Este é o roteiro para medir e
documentar. Preencha o resultado no `README.md` (seção "Quanto custou rodar").

## 1. De onde o custo vem

| Componente | Unidade de cobrança | O que faz subir |
|---|---|---|
| Cluster Kafka **Basic** | por hora ligado | ficar criado (mesmo sem tráfego) |
| **Conector** CDC | por hora por conector | conector `RUNNING` |
| **Flink** compute pool | por **CFU-hora** consumida | statements rodando, paralelismo, atualizações |
| **Schema Registry** | por hora no ambiente | ambiente ativo com SR habilitado |
| Armazenamento + rede | por GB | retenção dos tópicos e tráfego de saída |

Conceito que vale registrar: **custo = recurso ligado × tempo**. Um pipeline de
estudo pode custar poucos dólares por algumas horas — o erro caro é deixar tudo
ligado por dias. O `--max-cfu 10` do compute pool é um teto de gasto, não um piso.

## 2. Como medir

**Pela interface:** Confluent Cloud → *Billing & payments* → **Costs** / **Usage**.
Filtre pelo período em que você rodou o desafio e capture a tela (é a evidência
principal da camada 5).

**Pelo CLI** (se a sua versão tiver o comando):

```bash
confluent billing cost list --start 2026-10-01 --end 2026-10-14
confluent billing cost list --start 2026-10-01 --end 2026-10-14 -o json > evidencias/05-custos.json
```

**Estimativa rápida (para conferir a ordem de grandeza):**

```
horas de execução = (fim - início) em horas
custo_kafka       ≈ horas × preço_hora_do_cluster_Basic
custo_conector    ≈ horas × preço_hora_por_conector
custo_flink       ≈ CFUs_máximas × horas × preço_CFU_hora   (limite superior)
custo_SR          ≈ horas × preço_hora_do_Schema_Registry
custo_total       ≈ soma + armazenamento/rede
```

Consulte os preços vigentes em <https://www.confluent.io/confluent-cloud/pricing/>
(e o que a sua fatura mostra sempre manda).

## 3. Preencha aqui e cole no README

```text
Período do desafio: __/__/____ __:__  ->  __/__/____ __:__
Tempo total com recursos ligados: ____ horas

Cluster Kafka Basic ......... US$ ______
Conector CDC ................. US$ ______
Flink (compute pool) ......... US$ ______
Schema Registry .............. US$ ______
Outros (rede/armazenamento) .. US$ ______
-----------------------------------------
TOTAL ........................ US$ ______

Observações (o que mais pesou, o que daria para economizar):
```

## 4. Como derrubar (e parar de gastar)

```bash
bash scripts/99_destruir.sh
```

E confira depois, em *Billing → Costs*, se nenhum item continuou contando.
