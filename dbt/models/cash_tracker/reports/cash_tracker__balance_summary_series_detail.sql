WITH 
master_summary AS (
  SELECT
    *
  FROM {{ ref('cash_tracker__balance_summary') }}
),
master_flows AS (
    SELECT
        *
    FROM {{ ref('cash_tracker__flows_after_force_failing') }}
    WHERE ct_use = 1
),
series_flows AS (
    SELECT
        *
    FROM {{ ref('cash_tracker__flows_after_force_failing_series') }}
    WHERE ct_use = 1
),
accounts AS (
  SELECT
    *
  FROM {{ ref('stg_lucid__accounts') }}
),
series AS (
  SELECT
    *
  FROM {{ ref('stg_lucid__series') }}
),
final2 AS (
  SELECT
    report_date,
    cash_posting_transaction_timestamp,
    fund,
    flow_account,
    flow_status,
    series,
    transaction_action_id,
    flow_amount,
    flow_is_settled,
    ct_use,
    SUM(flow_amount) OVER (PARTITION BY report_date, fund, flow_account, transaction_action_id) AS total
  FROM master_flows
  UNION ALL
  SELECT
    report_date,
    cash_posting_transaction_timestamp,
    fund,
    flow_account,
    flow_status,
    series,
    transaction_action_id,
    flow_amount,
    flow_is_settled,
    ct_use,
    SUM(flow_amount) OVER (PARTITION BY report_date, fund, flow_account, transaction_action_id) AS total
  FROM series_flows
)

SELECT * FROM final2
