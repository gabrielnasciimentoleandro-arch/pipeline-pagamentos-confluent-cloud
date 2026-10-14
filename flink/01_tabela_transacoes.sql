-- =============================================================================
-- flink/01_tabela_transacoes.sql — Camada 4 (Processamento) — tabela de origem
-- =============================================================================
-- Espelha o tópico pgcdc.public.transacoes (Debezium + Schema Registry).
-- O envelope Debezium (before/after/op) é aberto automaticamente: as colunas
-- abaixo são os campos do "after" da tabela public.transacoes.
--
-- IMPORTANTE (tipos): o DDL definitivo é o que o Confluent Cloud gera em
--   Flink SQL -> "Create table from topic" -> pgcdc.public.transacoes
-- Se o tipo de criado_em vier diferente (ex.: TIMESTAMP_LTZ(3)), ajuste aqui e
-- registre no README o que a UI gerou. Este arquivo é o registro em código.
-- =============================================================================

CREATE TABLE transacoes (
    id              BIGINT,
    conta_id        BIGINT,
    cartao_id       STRING,
    valor           DOUBLE,
    moeda           STRING,
    estabelecimento STRING,
    cidade          STRING,
    status          STRING,
    criado_em       TIMESTAMP_LTZ(3),
    -- Tempo de evento: usamos o criado_em do banco como relógio da janela.
    -- 5 segundos de tolerância absorvem a latência do CDC + Kafka.
    WATERMARK FOR criado_em AS criado_em - INTERVAL '5' SECOND
) WITH (
    'connector'                = 'confluent',
    'kafka.topic'              = 'pgcdc.public.transacoes',
    'key.format'               = 'json-registry',
    'value.format'             = 'json-debezium-registry',
    -- retract: INSERT vira +I, UPDATE vira -U/+U, DELETE vira -D.
    -- É o modo correto para um changelog CDC (aceita retrações).
    'changelog.mode'           = 'retract',
    -- Lê o tópico desde o início: o snapshot inicial não é perdido.
    'scan.startup.mode'        = 'earliest-offset'
);

-- Conferência (evidência da camada 4):
--   SELECT id, cartao_id, valor, criado_em FROM transacoes LIMIT 10;
