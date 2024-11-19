{{
    config({
        "as_columnstore": false,
        "materialized": 'table',
        "post-hook": [
            "{{ create_nonclustered_index(columns = ['report_date']) }}",
            "{{ create_nonclustered_index(columns = ['fund']) }}",
            "{{ create_nonclustered_index(columns = ['series']) }}",
            "{{ create_nonclustered_index(columns = ['trade_id']) }}",
        ]
    })
}}

WITH
accounts AS (
  SELECT
    *
  FROM {{ ref('stg_lucid__accounts') }}
),
expected_flows AS (
  SELECT
    *
  FROM {{ ref('cash_tracker__expected_flows') }}
),
realloc_cash_flows AS (
  SELECT
    f.*,
    a.acct_number AS flow_acct_number
  FROM {{ ref('cash_tracker__flows_plus_allocations') }} AS f
  JOIN accounts AS a ON (f.fund = a.fund AND f.flow_account = a.acct_name)
  WHERE
    flow_security = '{{ var('CASH') }}'
    AND flow_status = '{{ var('AVAILABLE') }}'
    AND series = ''
    AND SUBSTRING(transaction_action_id,1,8) = 'REALLOC_'
),
combined AS (
  SELECT
    report_date,
    fund,
    series,
    [route],
    transaction_action_id,
    transaction_desc,
    flow_account,
    flow_acct_number,
    flow_security,
    flow_status,
    flow_amount,
    expected_is_settled AS flow_is_settled,
    expected_after_sweep AS flow_after_sweep,
    trade_id,
    counterparty,
    used_alloc,
    is_margin,
    is_hxswing,
    sweep_detected,
    generated_id,
    reference_number
  FROM expected_flows
  UNION ALL
  SELECT
    report_date,
    fund,
    series,
    [route],
    transaction_action_id,
    transaction_desc,
    flow_account,
    flow_acct_number,
    flow_security,
    flow_status,
    flow_amount,
    flow_is_settled,
    flow_after_sweep,
    trade_id,
    counterparty,
    used_alloc,
    0 AS is_margin,
    0 AS is_hxswing,
    NULL AS sweep_detected,
    generated_id,
    NULL AS reference_number
  FROM realloc_cash_flows
),
final AS (
  SELECT
    *
  FROM combined
)

SELECT * FROM final
