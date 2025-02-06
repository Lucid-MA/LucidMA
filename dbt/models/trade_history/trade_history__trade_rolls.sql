{{
    config({
        "as_columnstore": false,
        "materialized": 'table',
    })
}}

SELECT TRY_CAST(tradepiece AS BIGINT) AS trade_id, -- Ensure it's BIGINT
       TRY_CAST(alloc_of AS BIGINT)   AS alloc_of  -- Ensure it's BIGINT
FROM {{ ref('base_trade_history__trade_query') }}
WHERE alloc_of IS NOT NULL -- Ignore NULL values