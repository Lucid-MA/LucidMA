WITH
trades AS (
  SELECT
    *
  FROM {{ ref('base_cash_tracker__trades') }}
),
tradesfree AS (
  SELECT
    *,
    CASE
      WHEN report_date = start_date THEN 1
      ELSE 0
    END AS is_same_date
  FROM {{ ref('cash_tracker__tradesfree_query_on_date') }}
  WHERE action_id != '32939 TRANSMITTED'
  AND security = '{{var('CASH')}}'
),
new_rolling_on_by_date AS (
  SELECT
    report_date,
    trade_id
  FROM trades
  WHERE 
    (series = 'MASTER' OR is_also_master = 1)
    AND report_date = start_date
    AND is_same_date = 1
    AND is_roll_of = 1
    AND is_rolling_on = 1
),
series_trades AS (
  SELECT
    *,
    CASE
      WHEN trade_id IN (SELECT trade_id FROM new_rolling_on_by_date AS r WHERE r.report_date=trades.report_date AND r.trade_id = trades.trade_id) THEN 1
      ELSE 0
    END AS is_new_trade_rolling
  FROM trades
),
not_roll AS (
  SELECT
    report_date,
    fund,
    series,
    trade_id,
    used_alloc
  FROM series_trades st
  WHERE 
    st.series != 'MASTER' 
    AND st.is_new_trade_rolling = 0 
    AND st.is_trade_rolling = 0
    AND trade_id IS NOT NULL
    AND trade_id != ''
),
margin AS (
  SELECT
    report_date,
    fund,
    series,
    trade_id,
    used_alloc
  FROM tradesfree tf
  WHERE tf.series != 'MASTER'
),
final AS (
  SELECT
    'not_roll' AS route,
    *
  FROM not_roll
  UNION ALL
  SELECT
    'margin' AS route,
    *
  FROM margin
)

SELECT * FROM final
