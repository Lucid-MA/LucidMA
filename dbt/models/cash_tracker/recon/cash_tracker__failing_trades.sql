{{ 
  config(
    materialized='incremental',
    unique_key=['report_date','fund','transaction_action_id','flow_account'],
    pre_hook=[
      "DELETE FROM {{ this }} WHERE report_date = CAST(GETDATE() AS DATE)"
    ]
  )
}}

{% set backfill_date = var('backfill_date', '') %}

WITH
failing_trades AS (
  SELECT
    _flow_id,
    report_date,
    orig_report_date,
    fund,
    series,
    'failing-trades' AS [route],
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
    used_alloc,
    flow_acct_number
  FROM {{ ref('cash_tracker__expected_flows') }}
  WHERE (match_rank = 9999 OR expected_is_settled = 0)
)

{% if is_incremental() %}
  {% if backfill_date == '' %}
    SELECT * FROM failing_trades WHERE report_date = CAST(getdate() AS DATE)
  {% else %}
    SELECT * FROM failing_trades WHERE report_date = '{{ backfill_date }}'
  {% endif %}
{% else %}
  SELECT * FROM failing_trades 
  WHERE report_date > '2024-01-01' AND report_date < '2024-08-30'
{% endif %}
