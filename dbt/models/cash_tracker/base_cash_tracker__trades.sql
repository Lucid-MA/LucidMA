WITH
trades AS (
  SELECT
    *,
    quantity AS par
  FROM {{ ref('cash_tracker__trade_query_on_date') }}
),
rolling_on_by_date AS (
  SELECT
    report_date,
    roll_of
  FROM trades
  WHERE 
    (series = 'MASTER' OR is_also_master = 1)
    AND report_date = start_date
    AND roll_of != 0
    AND roll_of IS NOT NULL
),
rolling_on_static AS (
  SELECT
    report_date,
    roll_of
  FROM (values 
      (null,'131509'),
      (null,'131510'),
      (null,'131803'),
      (null,'131807'),
      (null,'131811'),
      (null,'132166'),
      (null,'132167'),
      (null,'132175'),
      (null,'132185'),
      (null,'134690'),
      (null,'134887'),
      (null,'134888'),
      (null,'134889'),
      (null,'135076')
  ) as t(report_date,roll_of)
),
final AS (
  SELECT
    *,
    CASE
      WHEN report_date = start_date THEN 1
      ELSE 0
    END AS is_same_date,
    CASE
      WHEN roll_of != 0 AND roll_of IS NOT NULL THEN 1
      ELSE 0
    END is_roll_of,
    CASE
      WHEN roll_of IN (SELECT roll_of FROM rolling_on_by_date AS r WHERE r.report_date=trades.report_date AND r.roll_of = trades.roll_of) THEN 1
      WHEN roll_of IN (SELECT roll_of FROM rolling_on_by_date AS r WHERE r.report_date IS NULL AND r.roll_of = trades.roll_of) THEN 1
      ELSE 0
    END AS is_rolling_on,
    CASE
      WHEN trade_id IN (SELECT roll_of FROM rolling_on_by_date AS r WHERE r.report_date=trades.report_date AND r.roll_of = trades.trade_id) THEN 1
      WHEN trade_id IN (SELECT roll_of FROM rolling_on_by_date AS r WHERE r.report_date IS NULL AND r.roll_of = trades.trade_id) THEN 1
      ELSE 0
    END AS is_trade_rolling,
    CASE
      WHEN trade_type = 0 THEN -1
      ELSE 1
    END AS modifier
  FROM trades
)

SELECT * FROM final
