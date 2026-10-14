-- =============================================================================
-- flink/02_tabela_contas.sql — Camada 4 (Processamento) — tabela de dimensão
-- =============================================================================
-- Espelha o tópico pgcdc.public.contas. É a tabela usada para ENRIQUECER a
-- transação: nome do titular, limite e cidade da conta entram no alerta.
--
-- DDL de referência: em Flink SQL -> "Create table from topic" -> pgcdc.public.contas
-- =============================================================================

CREATE TABLE contas (
    id        BIGINT,
    nome      STRING,
    limite    DOUBLE,
    cidade    STRING,
    criado_em TIMESTAMP_LTZ(3)
) WITH (
    'connector'      = 'confluent',
    'kafka.topic'    = 'pgcdc.public.contas',
    'key.format'     = 'json-registry',
    'value.format'   = 'json-debezium-registry',
    'changelog.mode' = 'retract',
    'scan.startup.mode' = 'earliest-offset'
);

-- Conferência:
--   SELECT id, nome, limite, cidade FROM contas;
