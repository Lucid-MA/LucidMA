WITH
flows AS (
  SELECT
    *
  FROM {{ ref('cash_tracker__flows') }}
),
manual_allocations AS (
  SELECT
    *
  FROM {{ ref('cash_tracker__manual_allocations') }}
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
    flow_settled,
    trade_id,
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
    flow_settled,
    trade_id,
    portion AS used_alloc
  FROM manual_allocations
)

SELECT * FROM final
