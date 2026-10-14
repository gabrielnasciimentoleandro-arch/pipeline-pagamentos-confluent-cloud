-- =============================================================================
-- flink/03_tabela_alertas.sql — Camada 4 (Processamento) — tabela de destino
-- =============================================================================
-- Espelha o tópico alertas.fraude (criado em scripts/02_criar_topico_alertas.sh).
--
-- Por que um sink de UPSERT (e não append):
--   a contagem de uma janela muda a cada evento novo que cai dentro dela.
--   Um fluxo que muda de valor não pode ser escrito em tópico append-only.
--   Declarando a chave primária (cartao_id, janela_inicio), cada alerta passa a
--   ser um ESTADO ATUALIZÁVEL: o mesmo cartão+janela refina o alerta em vez de
--   duplicar linhas. O consumidor sempre vê a versão mais recente.
-- =============================================================================

CREATE TABLE alertas_fraude (
    cartao_id      STRING,
    janela_inicio  STRING,
    tipo_alerta    STRING,
    conta_id       BIGINT,
    titular        STRING,
    limite_conta   DOUBLE,
    cidade_conta   STRING,
    cidade_ultima  STRING,
    qtd_transacoes BIGINT,
    valor_total    DOUBLE,
    valor_max      DOUBLE,
    janela_fim     STRING,
    PRIMARY KEY (cartao_id, janela_inicio) NOT ENFORCED
) WITH (
    'connector'      = 'confluent',
    'kafka.topic'    = 'alertas.fraude',
    'key.format'     = 'json-registry',
    'value.format'   = 'json-registry',
    'changelog.mode' = 'upsert'
);

-- Conferência (evidência da camada 4 / 5):
--   SELECT cartao_id, janela_inicio, qtd_transacoes, valor_total, cidade_ultima
--   FROM alertas_fraude;
