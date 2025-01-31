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
    '' AS series,
    MAX(CASE
      WHEN account = 'MAIN' THEN cash_balance
      ELSE 0.00
    END) AS cash_main,
    MAX(CASE
      WHEN account = 'MARGIN' THEN cash_balance
      ELSE 0.00
    END) AS cash_margin,
    MAX(CASE
      WHEN account = 'MANAGEMENT' THEN cash_balance
      ELSE 0.00
    END) AS cash_management
  FROM starting_balance
  GROUP BY balance_date, fund
  UNION
  SELECT
    report_date,
    fund,
    series,
    MAX(CASE
      WHEN flow_account = 'MAIN' THEN series_cash_eod
      ELSE 0.00
    END) AS cash_main,
    MAX(CASE
      WHEN flow_account = 'MARGIN' THEN series_cash_eod
      ELSE 0.00
    END) AS cash_margin,
    MAX(CASE
      WHEN flow_account = 'MANAGEMENT' THEN series_cash_eod
      ELSE 0.00
    END) AS cash_management
  FROM series_balance_history
  GROUP BY report_date, fund, series
  UNION
  SELECT
    report_date,
    fund,
    '' AS series,
    MAX(CASE
      WHEN acct_name = 'MAIN' THEN ct_cash_eod
      ELSE 0.00
    END) AS cash_main,
    MAX(CASE
      WHEN acct_name = 'MARGIN' THEN ct_cash_eod
      ELSE 0.00
    END) AS cash_margin,
    MAX(CASE
      WHEN acct_name = 'MANAGEMENT' THEN ct_cash_eod
      ELSE 0.00
    END) AS cash_management
  FROM master_summary
  GROUP BY report_date, fund
)

SELECT 
  report_date,
  fund,
  series,
  ROUND(cash_main, 2) AS cash_main,
  ROUND(cash_margin, 2) AS cash_margin,
  ROUND(cash_management, 2) AS cash_management
FROM final