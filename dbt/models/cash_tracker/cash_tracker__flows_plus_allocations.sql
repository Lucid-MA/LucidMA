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
    NULL AS counterparty,
    portion AS used_alloc
  FROM manual_allocations
)

SELECT * FROM final
