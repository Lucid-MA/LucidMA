WITH 
balance_history AS (
  SELECT
    report_date,
    fund,
    acct_name,
    account_number,
    [security],
    beginning_balance,
    net_activity,
    ending_balance
  FROM {{ ref('stg_lucid__balance_history') }}
  WHERE 1=1
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
    --AND TRIM(UPPER(transaction_type_name)) != 'INTERNAL MOVEMENT'
  GROUP BY report_date, short_acct_number
),
combined AS (
  SELECT
    bh.report_date,
    bh.fund,
    bh.acct_name,
    bh.account_number,
    bh.[security],
    bh.beginning_balance,
    bh.net_activity,
    COALESCE(bc.cash_amount, -1 * bs.sweep_amount) AS total_amount,
    bh.ending_balance,
    bc.cash_amount,
    bs.sweep_amount
  FROM balance_history AS bh 
  LEFT JOIN bnym_activity AS bc ON (bh.report_date=bc.report_date AND bh.account_number=bc.short_acct_number AND bh.security LIKE 'CASHUSD%')
  LEFT JOIN bnym_activity AS bs ON (bh.report_date=bs.report_date AND bh.account_number=bs.short_acct_number AND bh.security LIKE 'X9X9USD%')
)

SELECT 
  *
FROM combined
