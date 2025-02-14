WITH 
master_failing_trades AS (
  SELECT
    *
  FROM {{ ref('cash_tracker__flows_after_force_failing') }}
  WHERE report_date >= '2024-10-31'
  AND fund = 'PRIME'
  AND flow_account IN ('MAIN','MARGIN','SUBSCRIPTION')
  AND transaction_action_id NOT LIKE 'MMF_%'
  AND flow_is_settled = 0
),
series_failing_trades AS (
  SELECT
    *
  FROM {{ ref('cash_tracker__flows_after_force_failing_series') }}
  WHERE report_date >= '2024-10-31'
  AND fund = 'PRIME'
  AND flow_account IN ('MAIN','MARGIN','SUBSCRIPTION')
  AND transaction_action_id NOT LIKE 'MMF_%'
  AND flow_is_settled = 0
),
final AS (
  SELECT
    report_date,
    orig_report_date,
    fund,
    'MASTER' AS series,
    flow_account AS account,
    flow_amount AS amount,
    transaction_action_id as related_id
  FROM master_failing_trades
  UNION
  SELECT
    report_date,
    orig_report_date,
    fund,
    series,
    flow_account AS account,
    flow_amount AS amount,
    transaction_action_id as related_id
  FROM series_failing_trades
)

SELECT 
  report_date,
  orig_report_date,
  fund,
  series,
  account,
  amount,
  related_id
FROM final