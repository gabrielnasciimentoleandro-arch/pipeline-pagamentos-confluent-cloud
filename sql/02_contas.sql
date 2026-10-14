-- =============================================================================
-- 02_contas.sql — dados sintéticos das contas (dimensão)
-- =============================================================================
-- Como rodar:
--   psql "$DATABASE_URL" -f sql/02_contas.sql
--
-- Estes são os dados que o Flink vai usar para ENRIQUECER a transação
-- (nome do titular, limite e cidade da conta) na hora de montar o alerta.
-- =============================================================================

INSERT INTO public.contas (id, nome, limite, cidade) VALUES
    (1, 'Ana Souza',    2500.00, 'Presidente Prudente'),
    (2, 'Bruno Lima',   8000.00, 'São Paulo'),
    (3, 'Carla Dias',   1500.00, 'Campinas'),
    (4, 'Diego Alves', 12000.00, 'Curitiba'),
    (5, 'Elisa Rocha',  4000.00, 'Recife')
ON CONFLICT (id) DO UPDATE
    SET nome   = EXCLUDED.nome,
        limite = EXCLUDED.limite,
        cidade = EXCLUDED.cidade;

-- Cartão fixo por conta (facilita reproduzir a fraude e ler os alertas).
-- Ficam no gerador (scripts/gerador_transacoes.py) para manter o banco simples.
--
--   conta 1 -> 4111111111111111      conta 4 -> 4555555555555555
--   conta 2 -> 4222222222222222      conta 5 -> 4666666666666666
--   conta 3 -> 4333333333333333

SELECT id, nome, limite, cidade FROM public.contas ORDER BY id;
