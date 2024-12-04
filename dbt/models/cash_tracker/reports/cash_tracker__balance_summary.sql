WITH 
balance_history AS (
  SELECT
    report_date,
    fund,
    acct_name,
    account_number,
    cash_account_number,
    [security],
    beginning_balance,
    net_activity,
    ending_balance
  FROM {{ ref('stg_lucid__balance_history') }}
  WHERE 1=1
),
eod_history AS (
  SELECT
    DISTINCT
    {{ next_business_day('report_date') }} AS report_date,
    fund,
    acct_name,
    account_number,
    cash_account_number,
    [security],
    ending_balance AS prior_eod_balance
  FROM balance_history
),
account_history AS (
  SELECT
    DISTINCT
      report_date,
      fund,
      acct_name,
      account_number,
      cash_account_number
  FROM eod_history
  WHERE 1=1
  --AND cash_account_number LIKE '%8400'
),
bnym_activity AS (
  SELECT
    report_date,
    short_acct_number,
    SUM(CASE
      WHEN cusip_cins = 'X9USDDGCM' THEN local_amount
      WHEN cusip_cins = 'X9USDCMSH' THEN local_amount
      ELSE 0
    END) AS sweep_amount,
    SUM(local_amount) AS cash_amount
  FROM {{ ref('stg_lucid__cash_and_security_transactions') }}
  WHERE 1=1
  AND cash_account_number LIKE '%8400'
  GROUP BY report_date, short_acct_number
),
combined AS (
  SELECT
    ah.report_date,
    ah.fund,
    ah.acct_name,
    ah.account_number,
    COALESCE(ch.prior_eod_balance, 0) AS bnym_cash_bod,
    COALESCE(ba.cash_amount, 0) AS bnym_cash_activity,
    bhc.ending_balance AS cash_actual_eod,
    COALESCE(sh.prior_eod_balance, 0) AS bnym_sweep_bod,
    COALESCE(ba.sweep_amount, 0) * -1 AS bnym_sweep_activity,
    bhs.ending_balance AS sweep_actual_eod
  FROM account_history AS ah
  LEFT JOIN eod_history AS ch ON (ah.report_date=ch.report_date AND ah.account_number=ch.account_number AND ch.security LIKE 'CASHUSD%')
  LEFT JOIN eod_history AS sh ON (ah.report_date=sh.report_date AND ah.account_number=sh.account_number AND sh.security LIKE 'X9X9USD%')
  LEFT JOIN bnym_activity AS ba ON (ah.report_date=ba.report_date AND ah.account_number=ba.short_acct_number)
  LEFT JOIN balance_history AS bhc ON (ah.report_date=bhc.report_date AND ah.cash_account_number=bhc.cash_account_number AND bhc.security LIKE 'CASHUSD%')
  LEFT JOIN balance_history AS bhs ON (ah.report_date=bhs.report_date AND ah.cash_account_number=bhs.cash_account_number AND bhs.security LIKE 'X9X9USD%')
),
final AS (
  SELECT
    report_date,
    fund,
    acct_name,
    account_number,
    bnym_cash_bod,
    bnym_cash_activity,
    (bnym_cash_bod + bnym_cash_activity) AS bnym_cash_eod,
    cash_actual_eod,
    bnym_sweep_bod,
    bnym_sweep_activity,
    (bnym_sweep_bod + bnym_sweep_activity) AS bnym_sweep_eod,
    sweep_actual_eod
  FROM combined
)

SELECT 
  *
FROM final
