{{
    config({
        "as_columnstore": false,
        "materialized": 'table',
        "post-hook": [
            "{{ create_nonclustered_index(columns = ['report_date']) }}",
            "{{ create_nonclustered_index(columns = ['trade_id']) }}",
        ]
    })

}}

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
  FROM {{ ref('base_cash_tracker__trade_rolls') }}
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
