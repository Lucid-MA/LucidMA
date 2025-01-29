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
master_summary AS (
  SELECT
    *
  FROM {{ ref('cash_tracker__balance_summary') }}
),
master_flows AS (
    SELECT
        *
    FROM {{ ref('cash_tracker__flows_after_force_failing') }}
    WHERE ct_use = 1
),
cash_summary AS (
  SELECT
    *
  FROM {{ ref('stg_lucid__cash_balance_history') }}
  WHERE series != 'MASTER'
),
series_summary AS (
  SELECT
    *
  FROM {{ ref('cash_tracker__balance_summary_series') }}
),
series_flows AS (
    SELECT
        *
    FROM {{ ref('cash_tracker__flows_after_force_failing_series') }}
    WHERE ct_use = 1
),
accounts AS (
  SELECT
    *
  FROM {{ ref('stg_lucid__accounts') }}
),
series AS (
  SELECT
    *
  FROM {{ ref('stg_lucid__series') }}
),
final_settled_flows AS (
  SELECT
    s.report_date,
    s.fund,
    s.flow_account,
    s.series,
    a.acct_number AS account_number,
    TRY_CAST(SUM(
      CASE
        WHEN s.flow_is_settled = 1 THEN s.flow_amount
        ELSE 0
      END
    ) AS MONEY) AS series_settled_activity,
    TRY_CAST(SUM(
      CASE
        WHEN s.flow_is_settled = 0 THEN s.flow_amount
        ELSE 0
      END
    ) AS MONEY) AS series_unsettled_activity
  FROM series_flows AS s
  JOIN accounts AS a ON (s.fund = a.fund AND s.flow_account = a.acct_name)
  GROUP BY s.report_date, s.fund, s.flow_account, s.series, a.acct_number
),
final1 AS (
  SELECT
    TRY_CAST(
      m.report_date AS DATE
    ) AS report_date,
    m.fund,
    m.acct_name AS flow_account,
    s.series,
    m.account_number,
    COALESCE(p.series_settled_activity, 0) AS series_settled_activity,
    COALESCE(p.series_unsettled_activity, 0) AS series_unsettled_activity,
    CAST(
      SUM(p.series_settled_activity) OVER (PARTITION BY p.report_date, p.fund, p.account_number) 
    AS MONEY) AS series_total,
    CAST(m.ct_cash_flows AS MONEY) AS ct_cash_flows,
    CAST(m.bnym_cash_activity AS MONEY) AS bnym_cash_activity,
    CAST(m.ct_sweep_activity AS MONEY) AS ct_sweep_activity
  FROM master_summary AS m 
  JOIN series AS s ON (m.fund = s.fund)
  LEFT JOIN final_settled_flows AS p ON (
    m.report_date = p.report_date
    AND m.fund = p.fund
    AND m.account_number = p.account_number
    AND s.series = p.series
  )
  WHERE m.acct_name IN ('MAIN','MARGIN')
),
final2 AS (
  SELECT
    report_date,
    fund,
    series,
    flow_account,
    NULL AS series_cash_eod,
    NULL AS series_sweep_eod
  FROM final1 
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
    FROM series_summary WHERE report_date = CAST(getdate() AS DATE)
  {% else %}
    SELECT 
    report_date,
    fund,
    series,
    flow_account,
    (series_cash_bod + series_settled_activity + series_sweep_amount) AS series_cash_eod,
    (series_sweep_bod - series_sweep_amount) AS series_sweep_eod
    FROM series_summary WHERE report_date = '{{ backfill_date }}'
  {% endif %}
{% else %}
  SELECT
    balance_date AS report_date,
    fund,
    series,
    account AS flow_account,
    COALESCE(ROUND(cash_balance, 4), 0) AS series_cash_eod,
    COALESCE(ROUND(sweep_balance, 4), 0) AS series_sweep_eod
  FROM cash_summary
  WHERE balance_date > '2024-10-01' AND balance_date < '2024-11-01'
  AND fund IS NOT NULL
  AND series IS NOT NULL
{% endif %}
