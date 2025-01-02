WITH
buysell AS (
  SELECT
    *
  FROM {{ ref('cash_tracker__flows_buy_sell') }}
),
margin AS (
  SELECT
    *
  FROM {{ ref('cash_tracker__margin_flows') }}
),
manual_movements AS (
  SELECT
    *
  FROM {{ ref('cash_tracker__manual_movements') }}
),
cashpairoffs_agg AS (
  SELECT
    *
  FROM {{ ref('cash_tracker__cashpairoffs_summary') }}
),
final AS (
  SELECT 
    report_date,
    orig_report_date,
    fund,
    '' AS series,
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
    0 AS used_alloc
  FROM manual_movements
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
  flow_amount,
  flow_is_settled,
  flow_after_sweep,
  trade_id,
  counterparty,
  used_alloc
FROM final
