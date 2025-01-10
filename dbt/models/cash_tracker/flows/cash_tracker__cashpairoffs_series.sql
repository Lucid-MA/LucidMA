WITH
trades AS (
  SELECT
    *
  FROM {{ ref('base_cash_tracker__trades') }}
),
new_rolling_on_by_date AS (
  SELECT
    report_date,
    trade_id,
    roll_of,
    fund,
    series,
    is_also_master,
    is_same_date,
    is_roll_of
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
  WHERE series != 'MASTER' 
),
part1 AS (
  SELECT
    'Part1' AS route,
    CASE
      WHEN counterparty IN ('400CAP', '400CAPTX', 'CTVA', 'AGNC', 'CFCO', 'LMRMSTR') THEN counterparty + '~' + depository
      ELSE counterparty
    END AS counterparty2,
    -1 * modifier * ABS([money]) AS amount2,
    *
  FROM series_trades
  WHERE is_new_trade_rolling = 1
),
part2 AS (
  SELECT
    'Part2' AS route,
    CASE
      WHEN counterparty IN ('400CAP', '400CAPTX', 'CTVA', 'AGNC', 'CFCO', 'LMRMSTR') THEN counterparty + '~' + depository
      ELSE counterparty
    END AS counterparty2,
    modifier * ABS(end_money) AS amount2,
    *
  FROM series_trades
  WHERE is_new_trade_rolling = 0 AND is_trade_rolling = 1
),
final AS (
  SELECT
    *
  FROM part1
  UNION
  SELECT
    *
  FROM part2
)

SELECT * FROM final
