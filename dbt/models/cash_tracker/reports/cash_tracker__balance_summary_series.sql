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
series_summary AS (
  SELECT
    *
  FROM {{ ref('stg_lucid__balance_history_series') }}
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
    {{ next_business_day('report_date') }} AS report_date,
    fund,
    flow_account,
    series,
    TRY_CAST(COALESCE(series_cash_eod, 0) AS MONEY) AS cash_balance,
    TRY_CAST(COALESCE(series_sweep_eod, 0) AS MONEY) AS sweep_balance
  FROM series_summary
),
series_eod_balance AS (
  SELECT
    report_date,
    fund,
    flow_account,
    series,
    TRY_CAST(COALESCE(series_cash_eod, 0) AS MONEY) AS cash_balance,
    TRY_CAST(COALESCE(series_sweep_eod, 0) AS MONEY) AS sweep_balance
  FROM series_summary
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
series_combined AS (
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
    bod.cash_balance AS series_cash_bod,
    eod.cash_balance AS series_cash_eod,
    bod.sweep_balance AS series_sweep_bod,
    eod.sweep_balance AS series_sweep_eod,
    CAST(
      SUM(p.series_settled_activity) OVER (PARTITION BY m.report_date, m.fund, m.account_number) 
    AS MONEY) AS series_flows_total,
    (bod.cash_balance+bod.sweep_balance) AS series_acct_total,
    CAST(
      SUM((bod.cash_balance+bod.sweep_balance)) OVER (PARTITION BY m.report_date, m.fund, m.account_number) 
    AS MONEY) AS series_sum_total,
    (m.cash_actual_bod+m.sweep_actual_bod) AS master_acct_total,
    CAST(m.ct_cash_flows AS MONEY) AS ct_cash_flows,
    CAST(m.bnym_cash_activity AS MONEY) AS bnym_cash_activity,
    CAST(m.ct_sweep_activity AS MONEY) AS ct_sweep_activity,
    (m.ct_cash_flows+m.cash_actual_bod) AS ct_cash_total
  FROM master_summary AS m 
  JOIN series AS s ON (m.fund = s.fund)
  LEFT JOIN final_settled_flows AS p ON (
    m.report_date = p.report_date
    AND m.fund = p.fund
    AND m.account_number = p.account_number
    AND s.series = p.series
  )
  LEFT JOIN series_bod_balance AS bod ON (
    m.report_date = bod.report_date
    AND m.fund = bod.fund
    AND m.acct_name = bod.flow_account
    AND s.series = bod.series
  )
  LEFT JOIN series_eod_balance AS eod ON (
    m.report_date = eod.report_date
    AND m.fund = eod.fund
    AND m.acct_name = eod.flow_account
    AND s.series = eod.series
  )
),
calc_sweep_extra AS (
  SELECT
    *,
    COALESCE((series_acct_total/NULLIF(series_sum_total,0)), 0) AS series_total_ratio,
    (ct_sweep_activity + ct_cash_total) AS ct_sweep_extra
  FROM series_combined
),
calc_sweep_amt AS (
  SELECT
    *,
    CASE
      WHEN abs(ct_sweep_extra) > 0.01 THEN 
           - (series_cash_bod + series_settled_activity) + (series_total_ratio * ct_sweep_extra)
      ELSE - (series_cash_bod + series_settled_activity)
    END AS series_sweep_amount
  FROM calc_sweep_extra
),
final AS (
  SELECT
    *
  FROM calc_sweep_amt
)

SELECT * FROM final
