WITH
net_cash AS (
  SELECT
    *
  FROM {{ ref('cash_tracker__net_cash_by_counterparty') }}
),
net_cash_fund AS (
  SELECT
    fund AS cpc_fund,
    '' AS cpc_series,
    counterparty AS cpc_counterparty,
    net_cash AS cpc_balance,
    activity AS cpc_activity,
    *
  FROM net_cash
  WHERE (series = 'MASTER' OR is_also_master = 1)
),
net_cash_series AS (
  SELECT
    fund AS cpc_fund,
    series AS cpc_series,
    counterparty AS cpc_counterparty,
    net_cash AS cpc_balance,
    activity AS cpc_activity,
    *
  FROM net_cash
  WHERE (series != 'MASTER')
),
final AS (
  SELECT
    *
  FROM net_cash_fund
  UNION
  SELECT
    *
  FROM net_cash_series
)

SELECT * FROM final
