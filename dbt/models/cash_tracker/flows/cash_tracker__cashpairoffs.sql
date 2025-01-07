WITH
trades AS (
  SELECT
    *
  FROM {{ ref('base_cash_tracker__trades') }}
),
master_or_only_one_series AS (
  SELECT
    *
  FROM trades
  WHERE (series = 'MASTER' OR is_also_master = 1)
),
option1 AS (
  SELECT
    'Option1' AS route,
    CASE
      WHEN counterparty IN ('400CAP', '400CAPTX', 'CTVA', 'AGNC', 'CFCO', 'LMRMSTR') THEN counterparty + '~' + depository
      ELSE counterparty
    END AS counterparty2,
    -1 * modifier * ABS([money]) AS amount2,
    *
  FROM master_or_only_one_series
  WHERE (is_same_date = 1 AND is_roll_of = 1 AND is_rolling_on = 1)
),
option2 AS (
  SELECT
    'Option2' AS route,
    CASE
      WHEN counterparty IN ('400CAP', '400CAPTX', 'CTVA', 'AGNC', 'CFCO', 'LMRMSTR') THEN counterparty + '~' + depository
      ELSE counterparty
    END AS counterparty2,
    modifier * ABS(end_money) AS amount2,
    *
  FROM master_or_only_one_series
  WHERE (is_same_date = 0 OR is_roll_of = 0 OR is_rolling_on = 0)
    AND is_trade_rolling = 1
),
final AS (
  SELECT * FROM option1
  UNION
  SELECT * FROM option2
)

SELECT * FROM final
