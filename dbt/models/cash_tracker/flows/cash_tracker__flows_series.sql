WITH
buysell AS (
  SELECT
    *
  FROM {{ ref('cash_tracker__flows_buy_sell_series') }}
),
margin AS (
  SELECT
    *
  FROM {{ ref('cash_tracker__margin_flows_series') }}
),
cashpairoffs_agg AS (
  SELECT
    *
  FROM {{ ref('cash_tracker__cashpairoffs_summary_series') }}
),
manual_movements AS (
  SELECT
    *
  FROM {{ ref('cash_tracker__manual_movements_series') }}
),
manual_allocations AS (
  SELECT
    *
  FROM {{ ref('cash_tracker__manual_allocations') }}
),
final AS (
  SELECT 
    report_date,
    orig_report_date,
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
  FROM buysell
  UNION
  SELECT 
    report_date,
    orig_report_date,
    fund,
    series,
    [route],
    transaction_action_id,
    transaction_desc,
    flow_account, 
    flow_security,
    flow_status,
    flow_amount,
    NULL AS flow_is_settled,
    NULL AS flow_after_sweep,
    NULL AS trade_id,
    counterparty,
    used_alloc
  FROM cashpairoffs_agg
  UNION
  SELECT 
    report_date,
    orig_report_date,
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
  FROM margin
   UNION
  SELECT 
    report_date,
    orig_report_date,
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
  {{ dbt_utils.generate_surrogate_key([
    'report_date',
    'fund',
    'flow_account',
    'transaction_action_id',
    'flow_security',
    'flow_status'
  ]) }} AS _flow_id,
  report_date,
  orig_report_date,
  fund,
  series,
  [route],
  transaction_action_id,
  transaction_desc,
  flow_account, 
  flow_security,
  flow_status,
  ROUND(flow_amount, 4) AS flow_amount,
  flow_is_settled,
  flow_after_sweep,
  trade_id,
  counterparty,
  used_alloc
FROM final
