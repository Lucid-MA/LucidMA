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
flows AS (
  SELECT
    *
  FROM {{ ref('cash_tracker__flows') }}
  WHERE flow_account IS NOT NULL
),
failing_trades AS (
  SELECT
    _flow_id,
    {{ next_business_day('report_date') }} AS report_date,
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
  FROM {{ ref('stg_lucid__failing_trades') }}
),
final AS (
   SELECT 
    _flow_id,
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
  FROM flows
  UNION
  SELECT 
    _flow_id,
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
  FROM failing_trades
)

SELECT 
  _flow_id,
  ROW_NUMBER() OVER (ORDER BY report_date, CASE WHEN trade_id IS NULL THEN 1 ELSE 0 END, trade_id) AS generated_id,
  report_date,
  orig_report_date,
  final.fund,
  TRY_CAST(series AS NVARCHAR(50)) AS series,
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
  used_alloc,
  a.acct_number
FROM final
JOIN accounts AS a ON (final.fund = a.fund AND final.flow_account = a.acct_name)
