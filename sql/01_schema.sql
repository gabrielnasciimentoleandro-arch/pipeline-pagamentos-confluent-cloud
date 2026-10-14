-- =============================================================================
-- 01_schema.sql — Camada 2 (Ingestão) — modelo transacional que o CDC vai capturar
-- =============================================================================
-- Como rodar:
--   psql "$DATABASE_URL" -f sql/01_schema.sql
--
-- Requisitos do servidor Postgres para o CDC funcionar:
--   wal_level = logical          (obrigatório para replicação lógica / pgoutput)
--   max_replication_slots >= 1   (um slot por conector)
--   max_wal_senders >= 1
-- Em serviços gerenciados: Neon já vem com replicação lógica ligada;
-- na RDS/Aurora é preciso parametrizar rds.logical_replication = 1 e reiniciar.
-- =============================================================================

BEGIN;

DROP TABLE IF EXISTS public.transacoes CASCADE;
DROP TABLE IF EXISTS public.contas CASCADE;

-- -----------------------------------------------------------------------------
-- contas: a "dimensão" que será usada para enriquecer a transação no Flink
-- -----------------------------------------------------------------------------
CREATE TABLE public.contas (
    id        BIGINT PRIMARY KEY,
    nome      TEXT          NOT NULL,
    limite    NUMERIC(15,2) NOT NULL DEFAULT 5000.00,
    cidade    TEXT,
    criado_em TIMESTAMPTZ   NOT NULL DEFAULT now()
);

-- -----------------------------------------------------------------------------
-- transacoes: o "fato" — cada INSERT/UPDATE/DELETE aqui vira um evento no Kafka
-- -----------------------------------------------------------------------------
CREATE TABLE public.transacoes (
    id              BIGSERIAL PRIMARY KEY,
    conta_id        BIGINT        NOT NULL REFERENCES public.contas (id),
    cartao_id       TEXT          NOT NULL,
    valor           NUMERIC(15,2) NOT NULL CHECK (valor > 0),
    moeda           TEXT          NOT NULL DEFAULT 'BRL',
    estabelecimento TEXT,
    cidade          TEXT,
    status          TEXT          NOT NULL DEFAULT 'APROVADA',
    criado_em       TIMESTAMPTZ   NOT NULL DEFAULT now()
);

-- Índice pela chave da regra de fraude (cartão + tempo): útil para consultas locais
CREATE INDEX idx_transacoes_cartao_criado
    ON public.transacoes (cartao_id, criado_em DESC);

-- -----------------------------------------------------------------------------
-- REPLICA IDENTITY FULL
-- Garante que UPDATE e DELETE cheguem ao Kafka com o registro COMPLETO em "before"
-- (sem isso, eventos de DELETE só carregam a chave primária).
-- Trade-off: aumenta o volume de WAL — aceitável aqui, documentado no README.
-- -----------------------------------------------------------------------------
ALTER TABLE public.transacoes REPLICA IDENTITY FULL;
ALTER TABLE public.contas      REPLICA IDENTITY FULL;

-- -----------------------------------------------------------------------------
-- Publicação que o conector (Debezium/pgoutput) vai ler.
-- Com a publicação pré-criada, o conector não precisa adivinhar as tabelas.
-- -----------------------------------------------------------------------------
DROP PUBLICATION IF EXISTS dbz_pagamentos_pub;
CREATE PUBLICATION dbz_pagamentos_pub FOR TABLE public.transacoes, public.contas;

COMMIT;

-- Conferência rápida (evidência da camada):
--   SELECT slot_name, plugin, active FROM pg_replication_slots;
--   SELECT pubname, puballtables FROM pg_publication;
