-- =============================================================================
-- flink/04_alerta_fraude.sql — Camada 4 — o job de detecção de fraude
-- =============================================================================
-- Regra do desafio: 3+ transações do mesmo cartão dentro de 60 segundos => alerta.
--
-- Peças da regra:
--   TABLE(TUMBLE(TABLE transacoes, DESCRIPTOR(criado_em), INTERVAL '60' SECOND))
--       -> janela de 60 segundos por TEMPO DE EVENTO (criado_em, relógio do banco)
--   GROUP BY cartao_id + janela
--       -> agrupa por cartão dentro de cada janela
--   HAVING COUNT(*) >= 3
--       -> só interessa o que passou do limite
--   LEFT JOIN contas
--       -> enriquecimento com titular, limite e cidade da conta
-- =============================================================================

INSERT INTO alertas_fraude
SELECT
    t.cartao_id                                        AS cartao_id,
    DATE_FORMAT(window_start, 'yyyy-MM-dd HH:mm:ss')   AS janela_inicio,
    'FRAUDE_3_EM_60S'                                  AS tipo_alerta,
    MAX(t.conta_id)                                    AS conta_id,
    MAX(c.nome)                                        AS titular,
    MAX(c.limite)                                      AS limite_conta,
    MAX(c.cidade)                                      AS cidade_conta,
    MAX(t.cidade)                                      AS cidade_ultima,
    COUNT(*)                                           AS qtd_transacoes,
    SUM(t.valor)                                       AS valor_total,
    MAX(t.valor)                                       AS valor_max,
    DATE_FORMAT(window_end, 'yyyy-MM-dd HH:mm:ss')     AS janela_fim
FROM TABLE(
        TUMBLE(TABLE transacoes, DESCRIPTOR(criado_em), INTERVAL '60' SECOND)
     ) AS t
LEFT JOIN contas AS c
       ON t.conta_id = c.id
GROUP BY t.cartao_id, window_start, window_end
HAVING COUNT(*) >= 3;

-- -----------------------------------------------------------------------------
-- Variações úteis (documentadas, não ativas):
--
-- 1) Janela deslizante (HOP) de 60s com passo de 15s: pega rajadas que caem em
--    cima da fronteira entre duas janelas fixas, ao custo de alertas repetidos:
--      FROM TABLE(HOP(TABLE transacoes, DESCRIPTOR(criado_em),
--                     INTERVAL '15' SECOND, INTERVAL '60' SECOND)) AS t
--
-- 2) Usar o tempo de chegada do registro no Kafka em vez do relógio do banco
--    (se o timestamp do banco não for confiável ou vier muito no passado):
--      TUMBLE(TABLE transacoes, DESCRIPTOR($rowtime), INTERVAL '60' SECOND)
--
-- 3) Endurecer a regra: WHERE t.status = 'APROVADA' e LIMIT de valor
--    (ex.: HAVING COUNT(*) >= 3 AND SUM(t.valor) > 500).
-- -----------------------------------------------------------------------------
