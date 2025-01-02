{{
    config({
        "as_columnstore": false,
        "materialized": 'table',
        "post-hook": [
            "{{ create_nonclustered_index(columns = ['report_date']) }}",
        ]
    })
}}

WITH
tradesfree AS (
  SELECT
    *,
    CASE
      WHEN report_date = start_date THEN 1
      ELSE 0
    END AS is_same_date
  FROM {{ ref('cash_tracker__tradesfree_query_on_date') }}
  WHERE action_id != '32939 TRANSMITTED'
  AND security = '{{var('CASH')}}'
),
counterparty_cash AS (
  SELECT
    *
  FROM {{ ref('cash_tracker__counterparty_cash') }}
),
master_or_only_one_series AS (
  SELECT
    COALESCE(cpc.cpc_balance, 0) - COALESCE(cpc.cpc_activity, 0) AS marginBalBOD,
    COALESCE(cpc.cpc_balance, 0) AS marginBalEOD,
    'HXSWING' + t.trade_id AS swing_id,
    t.*
  FROM tradesfree t
  LEFT JOIN counterparty_cash cpc ON (
    t.report_date = cpc.report_date
    AND t.fund = cpc.cpc_fund
    AND cpc.cpc_series = ''
    AND t.counterparty = cpc.cpc_counterparty
  )
  WHERE (t.series = 'MASTER' OR t.is_also_master = 1)
),
margin_payment AS (
  SELECT
    'Margin1-payment' AS route,
    action_id AS transaction_action_id,
    'Pay ' + counterparty + ' margin' AS transaction_desc,
    'MAIN' AS flow_account, 
    security AS flow_security,
    '{{var('AVAILABLE')}}' AS flow_status,
    -(ABS(amount)) AS flow_amount,
    *
  FROM master_or_only_one_series
  WHERE trade_type = 22 AND is_same_date = 1
  UNION
  SELECT
    'Margin1-payment-swing-credit' AS route,
    swing_id AS transaction_action_id,
    swing_id AS transaction_desc,
    'MARGIN' AS flow_account, 
    security AS flow_security,
    '{{var('AVAILABLE')}}' AS flow_status,
    -(ABS(amount)) AS flow_amount,
    *
  FROM master_or_only_one_series
  WHERE trade_type = 22 AND is_same_date = 1
  AND marginBalBOD > 0 AND marginBalEOD > 0
  UNION
  SELECT
    'Margin1-payment-swing-debit' AS route,
    swing_id AS transaction_action_id,
    swing_id AS transaction_desc,
    'MAIN' AS flow_account, 
    security AS flow_security,
    '{{var('AVAILABLE')}}' AS flow_status,
    (ABS(amount)) AS flow_amount,
    *
  FROM master_or_only_one_series
  WHERE trade_type = 22 AND is_same_date = 1
  AND marginBalBOD > 0 AND marginBalEOD > 0
),
margin_receive_returning_margin AS (
  SELECT
    'Margin1-receive' AS route,
    action_id AS transaction_action_id,
    'Receive returned ' + counterparty + ' margin' AS transaction_desc,
    'MAIN' AS flow_account, 
    security AS flow_security,
    '{{var('AVAILABLE')}}' AS flow_status,
    (ABS(amount)) AS flow_amount,
    *
  FROM master_or_only_one_series
  WHERE trade_type = 22 AND is_same_date = 0
    UNION
  SELECT
    'Margin1-receive-swing-credit' AS route,
    swing_id + 'CLS' AS transaction_action_id,
    swing_id + 'CLS' AS transaction_desc,
    'MARGIN' AS flow_account, 
    security AS flow_security,
    '{{var('AVAILABLE')}}' AS flow_status,
    (ABS(amount)) AS flow_amount,
    *
  FROM master_or_only_one_series
  WHERE trade_type = 22 AND is_same_date = 0
  AND marginBalBOD > 0
  UNION
  SELECT
    'Margin1-receive-swing-debit' AS route,
    swing_id + 'CLS' AS transaction_action_id,
    swing_id + 'CLS' AS transaction_desc,
    'MAIN' AS flow_account, 
    security AS flow_security,
    '{{var('AVAILABLE')}}' AS flow_status,
    -(ABS(amount)) AS flow_amount,
    *
  FROM master_or_only_one_series
  WHERE trade_type = 22 AND is_same_date = 0
  AND marginBalBOD > 0
),
margin_receipt AS (
  SELECT
    'Margin2-receipt' AS route,
    action_id AS transaction_action_id,
    'Receive ' + counterparty + ' margin' AS transaction_desc,
    'MAIN' AS flow_account, 
    security AS flow_security,
    '{{var('AVAILABLE')}}' AS flow_status,
    (ABS(amount)) AS flow_amount,
    *
  FROM master_or_only_one_series
  WHERE trade_type = 23 AND is_same_date = 1
  UNION
  SELECT
    'Margin2-receipt-swing-credit' AS route,
    swing_id AS transaction_action_id,
    swing_id AS transaction_desc,
    'MAIN' AS flow_account, 
    security AS flow_security,
    '{{var('AVAILABLE')}}' AS flow_status,
    -(ABS(amount)) AS flow_amount,
    *
  FROM master_or_only_one_series
  WHERE trade_type = 23 AND is_same_date = 1
  AND marginBalEOD > 0
  UNION
  SELECT
    'Margin2-receipt-swing-debit' AS route,
    swing_id AS transaction_action_id,
    swing_id AS transaction_desc,
    'MARGIN' AS flow_account, 
    security AS flow_security,
    '{{var('AVAILABLE')}}' AS flow_status,
    (ABS(amount)) AS flow_amount,
    *
  FROM master_or_only_one_series
  WHERE trade_type = 23 AND is_same_date = 1
  AND marginBalEOD > 0
),
margin_return_received_margin AS (
  SELECT
    'Margin2-return' AS route,
    action_id AS transaction_action_id,
    'Return ' + counterparty + ' margin' AS transaction_desc,
    'MAIN' AS flow_account, 
    security AS flow_security,
    '{{var('AVAILABLE')}}' AS flow_status,
    -(ABS(amount)) AS flow_amount,
    *
  FROM master_or_only_one_series
  WHERE trade_type = 23 AND is_same_date = 0
  UNION
  SELECT
    'Margin2-return-swing-credit' AS route,
    swing_id + 'CLS' AS transaction_action_id,
    swing_id + 'CLS' AS transaction_desc,
    'MARGIN' AS flow_account, 
    security AS flow_security,
    '{{var('AVAILABLE')}}' AS flow_status,
    -(ABS(amount)) AS flow_amount,
    *
  FROM master_or_only_one_series
  WHERE trade_type = 23 AND is_same_date = 0
  AND marginBalBOD > 0
  UNION
  SELECT
    'Margin2-return-swing-debit' AS route,
    swing_id + 'CLS' AS transaction_action_id,
    swing_id + 'CLS' AS transaction_desc,
    'MAIN' AS flow_account, 
    security AS flow_security,
    '{{var('AVAILABLE')}}' AS flow_status,
    (ABS(amount)) AS flow_amount,
    *
  FROM master_or_only_one_series
  WHERE trade_type = 23 AND is_same_date = 0
  AND marginBalBOD > 0
),
margin_flows AS (
  SELECT
    *
  FROM margin_payment
  UNION
  SELECT
    *
  FROM margin_receive_returning_margin
  UNION
  SELECT
    *
  FROM margin_receipt
  UNION
  SELECT
    *
  FROM margin_return_received_margin
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
    NULL AS flow_is_settled,
    NULL AS flow_after_sweep,
    trade_id,
    counterparty,
    action_id,
    used_alloc 
  FROM margin_flows
)

SELECT * FROM final
