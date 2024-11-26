WITH
cash_recon AS (
  SELECT
    *
  FROM {{ ref('stg_lucid__cash_and_security_transactions') }}
  --WHERE TRIM(UPPER(transaction_type_name)) != 'INTERNAL MOVEMENT'
),
sweeps AS (
  SELECT
    report_date,
    fund,
    acct_name,
    SUM(local_amount) AS sweep_amount
  FROM cash_recon
  WHERE location_name = 'STIF LOCATIONS' AND transaction_type_name != 'DIVIDEND'
  GROUP BY report_date, fund, acct_name
)

SELECT * FROM sweeps
