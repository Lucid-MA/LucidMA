{{
    config({
        "as_columnstore": false,
        "materialized": 'table',
        "post-hook": [
            "{{ create_nonclustered_index(columns = ['report_date']) }}",
            "{{ create_nonclustered_index(columns = ['fund']) }}",
            "{{ create_nonclustered_index(columns = ['flow_account']) }}",
        ]
    })
}}

WITH
buysell AS (
  SELECT
    *
  FROM {{ ref('cash_tracker__flows_buy_sell') }}
),
series AS (
  SELECT
    *
  FROM {{ ref('cash_tracker__flows_series') }}
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
cashpairoffs AS (
  SELECT
    'PO ' + counterparty2 AS transaction_action_id,
    'PO ' + counterparty2 AS transaction_desc,
    'MAIN' AS flow_account, 
    '{{var('CASH')}}' AS flow_security,
    '{{var('AVAILABLE')}}' AS flow_status,
    amount2 AS flow_amount,
    NULL AS flow_is_settled,
    NULL AS flow_after_sweep,
    *
  FROM {{ ref('cash_tracker__cashpairoffs') }}
),
cashpairoffs_agg AS (
  SELECT 
    report_date,
    fund,
    transaction_action_id,
    transaction_desc,
    flow_account, 
    flow_security,
    flow_status,
    SUM(flow_amount) AS flow_amount,
    counterparty,
    MIN(used_alloc) AS used_alloc
  FROM cashpairoffs
  GROUP BY report_date, fund, transaction_action_id, transaction_desc, flow_account, flow_security, flow_status, counterparty
),
series_cashpairoffs AS (
  SELECT
    m.transaction_action_id,
    m.transaction_desc,
    m.flow_account, 
    m.flow_security,
    m.flow_status,
    CASE
      WHEN m.flow_account = 'EXPENSE' THEN 0.0
      ELSE CAST((m.amount2 * s.used_alloc) AS money)
    END AS flow_amount,
    m.flow_is_settled,
    m.flow_after_sweep,
    s.*
  FROM {{ ref('cash_tracker__cashpairoffs_series') }} AS s
  JOIN cashpairoffs AS m ON (s.counterparty2 = m.counterparty2 AND s.trade_id = m.trade_id)
),
final AS (
  SELECT 
    report_date,
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
  FROM series
  UNION
  SELECT 
    report_date,
    fund,
    '' AS series,
    'cashpairoffs_agg' AS [route],
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
  FROM series_cashpairoffs
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
  FROM margin
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
    0 AS used_alloc
  FROM manual_movements
)

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
FROM final
