{{
    config({
        "as_columnstore": false,
        "materialized": 'table',
    })
}}

WITH TradeHistory AS (
    -- Start from the most recent trades (trades not found in alloc_of)
    SELECT trade_id,
           alloc_of,
           trade_id AS root_trade_id
    FROM {{ ref('trade_history__trade_rolls') }}
    WHERE trade_id NOT IN (SELECT alloc_of FROM {{ ref('trade_history__trade_rolls') }} WHERE alloc_of IS NOT NULL)

    UNION ALL

    -- Recursively find previous trades
    SELECT t.trade_id,
           t.alloc_of,
           th.root_trade_id
    FROM {{ ref('trade_history__trade_rolls') }} t
             INNER JOIN TradeHistory th
                        ON t.trade_id = th.alloc_of)
-- Aggregate and SORT roll history (from largest to smallest)
SELECT root_trade_id AS           trade_id,
       STRING_AGG(alloc_of, ', ') WITHIN GROUP (ORDER BY alloc_of DESC) AS roll_of_list
FROM TradeHistory
WHERE alloc_of IS NOT NULL
GROUP BY root_trade_id;