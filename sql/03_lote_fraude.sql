-- =============================================================================
-- 03_lote_fraude.sql — Camada 4 (Processamento) — dispara a regra de fraude
-- =============================================================================
-- Como rodar:
--   psql "$DATABASE_URL" -f sql/03_lote_fraude.sql
--
-- Regra do desafio: 3 transações do MESMO cartão em até 60 segundos => alerta.
-- Este script insere exatamente 3 transações no cartão da conta 1 em poucos
-- segundos. O CDC captura, o Flink agrega na janela de 60s e publica o alerta.
--
-- A terceira transação acontece em outra cidade: o alerta enriquecido mostra
-- isso (evidência de que o join com a tabela de contas funcionou).
-- =============================================================================

BEGIN;

INSERT INTO public.transacoes
    (conta_id, cartao_id, valor, moeda, estabelecimento, cidade, status)
VALUES
    (1, '4111111111111111',  89.90, 'BRL', 'Padaria Central',   'Presidente Prudente', 'APROVADA'),
    (1, '4111111111111111', 149.90, 'BRL', 'Loja de Roupas',    'Presidente Prudente', 'APROVADA'),
    (1, '4111111111111111', 999.00, 'BRL', 'Eletrônicos Online', 'Miami',              'APROVADA');

COMMIT;

-- Conferência rápida (evidência da camada):
--   SELECT cartao_id, count(*), min(criado_em), max(criado_em)
--   FROM public.transacoes
--   WHERE criado_em > now() - interval '2 minutes'
--   GROUP BY cartao_id;
