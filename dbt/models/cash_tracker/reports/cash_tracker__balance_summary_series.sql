{% set series_names = dbt_utils.get_column_values(table=ref('stg_lucid__series'), column='series') %}
{% set sum_columns = [] %}
{% for name in series_names %}
  {% do sum_columns.append('[' ~ name ~ '_settled]') %}
{% endfor %}
{% set total_activity = sum_columns | join(' + ') %}

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
series_bod_balance AS (
  SELECT
    {{ next_business_day('balance_date') }} AS report_date,
    fund,
    series,
    account,
    COALESCE(cash_balance, 0) AS cash_balance,
    COALESCE(sweep_balance, 0) AS sweep_balance,
    COALESCE(projected_total_balance, 0) AS projected_total_balance
  FROM cash_summary
),
series_eod_balance AS (
  SELECT
    balance_date AS report_date,
    fund,
    series,
    account,
    COALESCE(cash_balance, 0) AS cash_balance,
    COALESCE(sweep_balance, 0) AS sweep_balance,
    COALESCE(projected_total_balance, 0) AS projected_total_balance
  FROM cash_summary
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
flows_pivot AS (
  SELECT
    report_date,
    fund,
    flow_account AS acct_name,
    account_number,
    {{ dbt_utils.pivot(
      'series',
      dbt_utils.get_column_values(table=ref('stg_lucid__series'), column='series'),
      agg='max',
      then_value='series_settled_activity',
      suffix='_settled'
  ) }}
  FROM final_settled_flows
  GROUP BY report_date, fund, flow_account, account_number
),
final1 AS (
  SELECT
    p.*,
    TRY_CAST(bod.cash_balance AS MONEY) AS cash_actual_bod,
    TRY_CAST((eod.cash_balance - bod.cash_balance) AS MONEY) AS cash_actual_activity,
    TRY_CAST(eod.cash_balance AS MONEY) AS cash_actual_eod,
    TRY_CAST(bod.sweep_balance AS MONEY) AS sweep_actual_bod,
    TRY_CAST((eod.sweep_balance - bod.sweep_balance) AS MONEY) AS sweep_actual_activity,
    TRY_CAST(eod.sweep_balance AS MONEY) AS sweep_actual_eod,
    CAST(
      SUM(p.series_settled_activity) OVER (PARTITION BY p.report_date, p.fund, p.account_number) 
    AS MONEY) AS series_total,
    CAST(m.ct_cash_flows AS MONEY) AS ct_cash_flows,
    CAST(m.ct_sweep_activity AS MONEY) AS ct_sweep_activity
  FROM final_settled_flows AS p
  LEFT JOIN master_summary AS m ON (
    p.report_date = m.report_date
    AND p.fund = m.fund
    AND p.account_number = m.account_number
  )
  LEFT JOIN series_eod_balance AS eod ON (
    p.report_date = eod.report_date
    AND p.fund = eod.fund
    AND p.flow_account = eod.account
    AND p.series = eod.series
  )
  LEFT JOIN series_bod_balance AS bod ON (
    p.report_date = bod.report_date
    AND p.fund = bod.fund
    AND p.flow_account = bod.account
    AND p.series = bod.series
  )
),
final2 AS (
SELECT
  report_date,
  fund,
  flow_account,
  series,
  account_number,
  series_settled_activity,
  series_unsettled_activity,
  cash_actual_bod,
  cash_actual_activity AS cash_actual_change,
  cash_actual_eod,
  sweep_actual_bod,
  sweep_actual_activity AS sweep_actual_change,
  sweep_actual_eod,
  series_total,
  CAST(
      SUM(cash_actual_activity) OVER (PARTITION BY report_date, fund, account_number) 
  AS MONEY) AS cash_actual_total,
  CAST(
      SUM(sweep_actual_activity) OVER (PARTITION BY report_date, fund, account_number) 
  AS MONEY) AS sweep_actual_total,
  ct_cash_flows,
  ct_sweep_activity,
  CAST((cash_actual_activity + sweep_actual_activity) AS MONEY) AS total_cash_activity,
  CAST(abs(series_total - ct_cash_flows) AS MONEY) AS series_flows_diff
FROM final1
)

SELECT
  report_date,
  fund,
  flow_account,
  series,
  account_number,
  series_settled_activity,
  (series_settled_activity/ct_sweep_activity) AS series_settled_sweep_percent,
  (cash_actual_change/cash_actual_total) AS series_cash_percent,
  (series_settled_activity/series_total) AS series_settled_total_percent,
  ((cash_actual_change+sweep_actual_change)/ct_sweep_activity) AS series_total_percent,
  (sweep_actual_change/sweep_actual_total) AS series_sweep_percent,
  series_unsettled_activity,
  cash_actual_bod,
  cash_actual_change,
  cash_actual_eod,
  sweep_actual_bod,
  sweep_actual_change,
  sweep_actual_eod,
  series_total,
  cash_actual_total,
  sweep_actual_total,
  ct_cash_flows,
  ct_sweep_activity,
  (cash_actual_total-sweep_actual_total) AS total_change,
  total_cash_activity,
  series_flows_diff
FROM final2
