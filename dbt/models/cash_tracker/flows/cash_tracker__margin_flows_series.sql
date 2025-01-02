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
margin_flows AS (
  SELECT
    *
  FROM {{ ref('cash_tracker__margin_flows') }}
),
series AS (
  SELECT
    'Margin-series' AS route,
    mf.transaction_action_id,
    mf.transaction_desc,
    mf.flow_account, 
    mf.flow_security,
    mf.flow_status,
    mf.flow_amount AS master_flow_amount,
    CAST(CASE
      WHEN mf.flow_account = 'EXPENSE' THEN 0.0
      ELSE (mf.flow_amount * tf.used_alloc)
    END AS MONEY) AS flow_amount,
    tf.*
  FROM tradesfree tf
  LEFT JOIN margin_flows mf ON (
    tf.action_id = mf.action_id
    AND tf.report_date = mf.report_date
  )
  WHERE tf.series != 'MASTER'
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
    master_flow_amount,
    flow_amount,
    NULL AS flow_is_settled,
    NULL AS flow_after_sweep,
    trade_id,
    counterparty,
    action_id,
    used_alloc 
  FROM series
)

SELECT * FROM final
