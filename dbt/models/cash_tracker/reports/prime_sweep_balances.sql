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
    balance_date AS report_date,
    fund,
    'MASTER' AS series,
    MAX(CASE
      WHEN account = 'MAIN' THEN sweep_balance
      ELSE 0.00
    END) AS sweep_main,
    MAX(CASE
      WHEN account = 'MARGIN' THEN sweep_balance
      ELSE 0.00
    END) AS sweep_margin,
    MAX(CASE
      WHEN account = 'MANAGEMENT' THEN sweep_balance
      ELSE 0.00
    END) AS sweep_management
  FROM starting_balance
  GROUP BY balance_date, fund
  UNION
  SELECT
    report_date,
    fund,
    series,
    MAX(CASE
      WHEN flow_account = 'MAIN' THEN series_sweep_eod
      ELSE 0.00
    END) AS sweep_main,
    MAX(CASE
      WHEN flow_account = 'MARGIN' THEN series_sweep_eod
      ELSE 0.00
    END) AS sweep_margin,
    MAX(CASE
      WHEN flow_account = 'MANAGEMENT' THEN series_sweep_eod
      ELSE 0.00
    END) AS sweep_management
  FROM series_balance_history
  GROUP BY report_date, fund, series
  UNION
  SELECT
    report_date,
    fund,
    'MASTER' AS series,
    MAX(CASE
      WHEN acct_name = 'MAIN' THEN ct_sweep_eod
      ELSE 0.00
    END) AS sweep_main,
    MAX(CASE
      WHEN acct_name = 'MARGIN' THEN ct_sweep_eod
      ELSE 0.00
    END) AS sweep_margin,
    MAX(CASE
      WHEN acct_name = 'MANAGEMENT' THEN ct_sweep_eod
      ELSE 0.00
    END) AS sweep_management
  FROM master_summary
  GROUP BY report_date, fund
)

SELECT 
  report_date,
  fund,
  series,
  ROUND(sweep_main, 2) AS sweep_main,
  ROUND(sweep_margin, 2) AS sweep_margin,
  ROUND(sweep_management, 2) AS sweep_management
FROM final