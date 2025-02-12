WITH 
master_summary AS (
  SELECT
    *
  FROM {{ ref('cash_tracker__balance_summary') }}
  WHERE report_date >= '2024-10-31'
  AND fund = 'PRIME'
  AND acct_name IN ('MAIN','MARGIN','MANAGEMENT')
),
series_balance_history AS (
  SELECT
    *
  FROM {{ ref('cash_tracker__balance_history_series') }}
  WHERE fund = 'PRIME'
),
series_balance_summary AS (
  SELECT
    *
  FROM {{ ref('cash_tracker__balance_summary_series') }}
  WHERE fund = 'PRIME'
),
starting_balance AS (
  SELECT
    *
  FROM {{ ref('series_balance') }}
  WHERE balance_date = '2024-10-31'
  AND fund = 'PRIME'
  AND series = 'MASTER'
),
final AS (
  SELECT
    sh.*,
    CAST(SUM(sh.series_cash_eod) OVER (PARTITION BY sh.report_date, sh.fund, sh.flow_account) 
    AS MONEY) AS series_cash_total,
    ms.cash_actual_eod,
    CAST(SUM(sh.series_sweep_eod) OVER (PARTITION BY sh.report_date, sh.fund, sh.flow_account) 
    AS MONEY) AS series_sweep_total,
    ms.sweep_actual_eod,
    ms.diff_cash_eod
  FROM series_balance_history AS sh
  LEFT JOIN master_summary AS ms 
    ON (
      sh.report_date = ms.report_date
      AND sh.fund = ms.fund
      AND sh.flow_account = ms.acct_name
    )
)

SELECT 
  report_date,
  fund,
  series,
  flow_account,
  series_cash_eod,
  series_sweep_eod,
  series_cash_total,
  cash_actual_eod,
  (series_cash_total - cash_actual_eod) AS cash_diff,
  diff_cash_eod,
  series_sweep_total,
  sweep_actual_eod,
  (series_sweep_total - sweep_actual_eod) AS sweep_diff
FROM final