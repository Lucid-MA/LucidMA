{{
    config({
        "as_columnstore": false,
        "materialized": 'table',
        "post-hook": [
            "{{ create_nonclustered_index(columns = ['report_date']) }}",
            "{{ create_nonclustered_index(columns = ['fund']) }}",
            "{{ create_nonclustered_index(columns = ['trade_id']) }}",
        ]
    })
}}

WITH
flows AS (
  SELECT
    *
  FROM {{ ref('cash_tracker__flows') }}
  WHERE flow_account IS NOT NULL
),
manual_allocations AS (
  SELECT
    *
  FROM {{ ref('cash_tracker__manual_allocations') }}
  WHERE flow_account IS NOT NULL
),
failing_trades AS (
  SELECT
    report_date,
    fund,
    '' AS series,
    'failing-trade' AS [route],
    related_id AS transaction_action_id,
    [description] AS transaction_desc,
    acct_name AS flow_account, 
    '{{var('CASH')}}' AS flow_security,
    '{{var('AVAILABLE')}}' AS flow_status,
    amount AS flow_amount,
    0 AS flow_is_settled,
    0 AS flow_after_sweep,
    helix_id AS trade_id,
    counterparty,
    NULL AS used_alloc
  FROM {{ ref('stg_lucid__failing_trades') }}
),
final AS (
   SELECT 
    report_date,
    fund,
    series,
    [route],
    transaction_action_id,
    transaction_desc,
    flow_account, 
    flow_security,
    flow_status,
    flow_amount,
    flow_is_settled,
    flow_after_sweep,
    trade_id,
    counterparty,
    used_alloc
  FROM flows
  UNION
  SELECT 
    report_date,
    fund,
    series,
    [route],
    transaction_action_id,
    transaction_desc,
    flow_account, 
    flow_security,
    flow_status,
    flow_amount,
    flow_is_settled,
    flow_after_sweep,
    trade_id,
    counterparty,
    used_alloc
  FROM failing_trades
  UNION
  SELECT 
    report_date,
    fund,
    series,
    [route],
    transaction_action_id,
    transaction_desc,
    flow_account, 
    flow_security,
    flow_status,
    flow_amount,
    flow_is_settled,
    flow_after_sweep,
    trade_id,
    NULL AS counterparty,
    portion AS used_alloc
  FROM manual_allocations
)

SELECT 
  ROW_NUMBER() OVER (ORDER BY report_date, CASE WHEN trade_id IS NULL THEN 1 ELSE 0 END, trade_id) AS generated_id,
  report_date,
  fund,
  series,
  [route],
  TRIM(transaction_action_id) AS transaction_action_id,
  TRIM(transaction_desc) AS transaction_desc,
  flow_account, 
  flow_security,
  flow_status,
  CAST(ROUND(flow_amount,2) AS MONEY) AS flow_amount,
  flow_is_settled,
  flow_after_sweep,
  trade_id,
  counterparty,
  used_alloc
FROM final
