WITH 
master_summary AS (
  SELECT
    *
  FROM {{ ref('cash_tracker__balance_summary') }}
  WHERE report_date >= '2024-10-31'
  AND fund = 'PRIME'
  AND acct_name IN ('MAIN','MARGIN')
),
series_balance_history AS (
  SELECT
    *
  FROM {{ ref('cash_tracker__balance_history_series') }}
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
failing_trades AS (
  SELECT
    report_date,
    fund,
    series,
    account,
    SUM(amount) AS total_failing_amount
  FROM {{ ref('cash_summary_failing_trades') }}
  GROUP BY report_date, fund, series, account
),
final AS (
  SELECT
    balance_date AS report_date,
    fund,
    'MASTER' AS series,
    account,
    cash_balance,
    sweep_balance,
    projected_total_balance
  FROM starting_balance
  UNION
  SELECT
    series_balance_history.report_date,
    series_balance_history.fund,
    series_balance_history.series,
    series_balance_history.flow_account AS account,
    series_cash_eod AS cash_balance,
    series_sweep_eod AS sweep_balance,
    (series_cash_eod + series_sweep_eod + COALESCE(failing_trades.total_failing_amount,0)) AS projected_total_balance
  FROM series_balance_history
  LEFT JOIN failing_trades
    ON (
      series_balance_history.report_date = failing_trades.report_date
      AND series_balance_history.fund = failing_trades.fund
      AND series_balance_history.series = failing_trades.series
      AND series_balance_history.flow_account = failing_trades.account
    )
  UNION
  SELECT
    master_summary.report_date,
    master_summary.fund,
    'MASTER' AS series,
    master_summary.acct_name AS account,
    ct_cash_eod AS cash_balance,
    ct_sweep_eod AS sweep_balance,
    (ct_cash_eod + ct_sweep_eod + COALESCE(failing_trades.total_failing_amount,0)) AS projected_total_balance
  FROM master_summary
  LEFT JOIN failing_trades
    ON (
      master_summary.report_date = failing_trades.report_date
      AND master_summary.fund = failing_trades.fund
      AND failing_trades.series = 'MASTER'
      AND master_summary.acct_name = failing_trades.account
    )
)

SELECT 
  report_date,
  fund,
  series,
  account,
  CAST(cash_balance AS MONEY) AS cash_balance,
  CAST(sweep_balance AS MONEY) AS sweep_balance,
  CAST(projected_total_balance AS MONEY) AS projected_total_balance
FROM final