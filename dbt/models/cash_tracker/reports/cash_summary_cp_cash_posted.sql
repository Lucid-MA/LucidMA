WITH
cp_cash AS (
  SELECT
    *
  FROM {{ ref('cash_tracker__counterparty_cash') }}
),
final AS (
  SELECT
    report_date,
    cpc_fund AS fund,
    CASE
      WHEN cpc_series = '' THEN 'MASTER'
      ELSE cpc_series
    END AS series,
    cpc_counterparty AS counterparty,
    cpc_balance as amount
  FROM cp_cash
  WHERE cpc_balance < 0
)

SELECT
  report_date,
  fund,
  series,
  counterparty,
  amount
FROM final
