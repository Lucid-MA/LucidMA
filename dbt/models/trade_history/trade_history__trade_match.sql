{{
    config({
        "as_columnstore": false,
        "materialized": 'table',
    })
}}

WITH trade_roll_history AS (
    SELECT trade_id,
           roll_of_list
    FROM {{ ref('trade_history__trade_roll_history') }}
),

distinct_helix AS (
    -- Cast helix_id to BIGINT for numeric comparison
    SELECT DISTINCT CAST(helix_id AS BIGINT) AS helix_id
    FROM {{ ref('stg_lucid__cash_and_security_transactions') }}
    WHERE helix_id IS NOT NULL
      AND transaction_type_name = 'BUY'
),

SplitRolls AS (
    -- Cast split values to BIGINT
    SELECT
        trh.trade_id,
        trh.roll_of_list,
        CAST(value AS BIGINT) AS roll_of  -- No TRY_CAST needed if values are confirmed numeric
    FROM trade_roll_history trh
    CROSS APPLY STRING_SPLIT(
        CASE
            WHEN CHARINDEX(',', trh.roll_of_list) > 0 THEN trh.roll_of_list
            WHEN ISNULL(trh.roll_of_list, '') <> '' THEN trh.roll_of_list + ','
            ELSE ''
        END,
        ','
    )
    WHERE ISNULL(trh.roll_of_list, '') <> ''
),

MatchedTrades AS (
    -- Match on BIGINT and compute numeric MAX()
    SELECT
        trh.trade_id,
        trh.roll_of_list,
        MAX(dh.helix_id) AS match_id  -- Now correctly returns the largest numeric value
    FROM SplitRolls trh
    INNER JOIN distinct_helix dh
        ON trh.roll_of = dh.helix_id  -- Both are BIGINT
    GROUP BY trh.trade_id, trh.roll_of_list
)

SELECT
    trh.trade_id,
    trh.roll_of_list,
    mt.match_id,
    CASE
        WHEN mt.match_id IS NOT NULL THEN 'True'
        ELSE 'False'
    END AS status
FROM trade_roll_history trh
LEFT JOIN MatchedTrades mt
    ON trh.trade_id = mt.trade_id;