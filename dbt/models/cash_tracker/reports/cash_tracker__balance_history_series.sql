{{ 
  config(
    materialized='incremental',
    unique_key=['report_date','fund','flow_account','series'],
    pre_hook=[
      "DELETE FROM {{ this }} WHERE report_date = CAST(GETDATE() AS DATE)"
    ]
  )
}}

{% set backfill_date = var('backfill_date', '') %}

WITH 
series_inital_balances AS (
  SELECT
    *
  --FROM {{ ref('stg_lucid__cash_balance_history') }}
  FROM {{ ref('series_balance') }}
  WHERE series != 'MASTER'
),
series_summary AS (
  SELECT
    *
  FROM {{ ref('cash_tracker__balance_summary_series') }}
)

{% if is_incremental() %}
  {% if backfill_date == '' %}
    SELECT 
    report_date,
    fund,
    series,
    flow_account,
    (series_cash_bod + series_settled_activity + series_sweep_amount) AS series_cash_eod,
    (series_sweep_bod - series_sweep_amount) AS series_sweep_eod
    FROM series_summary 
    WHERE report_date = CAST(getdate() AS DATE) AND flow_account IN ('MAIN','MARGIN','MANAGEMENT')
  {% else %}
    SELECT 
    report_date,
    fund,
    series,
    flow_account,
    (series_cash_bod + series_settled_activity + series_sweep_amount) AS series_cash_eod,
    (series_sweep_bod - series_sweep_amount) AS series_sweep_eod
    FROM series_summary 
    WHERE report_date = '{{ backfill_date }}' AND flow_account IN ('MAIN','MARGIN','MANAGEMENT')
  {% endif %}
{% else %}
  SELECT
    balance_date AS report_date,
    fund,
    series,
    account AS flow_account,
    COALESCE(ROUND(cash_balance, 4), 0) AS series_cash_eod,
    COALESCE(ROUND(sweep_balance, 4), 0) AS series_sweep_eod
  FROM series_inital_balances
  WHERE balance_date > '2024-10-01' AND balance_date < '2024-11-01'
  AND fund IS NOT NULL
  AND series IS NOT NULL
  AND account IN ('MAIN','MARGIN','MANAGEMENT')
{% endif %}
