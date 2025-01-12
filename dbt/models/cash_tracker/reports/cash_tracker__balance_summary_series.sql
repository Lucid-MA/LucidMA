{% set series_names = dbt_utils.get_column_values(table=ref('stg_lucid__series'), column='series') %}
{% set sum_columns = [] %}
{% for name in series_names %}
  {% do sum_columns.append('[' ~ name ~ '_settled]') %}
{% endfor %}
{% set total_activity = sum_columns | join(' + ') %}

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
final_settled_flows AS (
  SELECT
    s.report_date,
    s.fund,
    s.flow_account,
    s.series,
    a.acct_number AS account_number,
    SUM(
      CASE
        WHEN s.flow_is_settled = 1 THEN s.flow_amount
        ELSE 0
      END
    ) AS series_settled_activity,
    SUM(
      CASE
        WHEN s.flow_is_settled = 0 THEN s.flow_amount
        ELSE 0
      END
    ) AS series_unsettled_activity
  FROM series_flows AS s
  JOIN accounts AS a ON (s.fund = a.fund AND s.flow_account = a.acct_name)
  GROUP BY s.report_date, s.fund, s.flow_account, s.series, a.acct_number
),
flows_pivot AS (
  SELECT
    report_date,
    fund,
    flow_account AS acct_name,
    account_number,
    {{ dbt_utils.pivot(
      'series',
      dbt_utils.get_column_values(table=ref('stg_lucid__series'), column='series'),
      agg='max',
      then_value='series_settled_activity',
      suffix='_settled'
  ) }}
  FROM final_settled_flows
  GROUP BY report_date, fund, flow_account, account_number
),
final AS (
  SELECT
    p.*,
    CAST(
      SUM(p.series_settled_activity) OVER (PARTITION BY p.report_date, p.fund, p.account_number) 
    AS MONEY) AS series_total,
    CAST(m.ct_cash_flows AS MONEY) AS ct_cash_flows
  FROM final_settled_flows AS p
  LEFT JOIN master_summary AS m ON (
    p.report_date = m.report_date
    AND p.fund = m.fund
    AND p.account_number = m.account_number
  )
)

SELECT
  *,
  CAST(abs(series_total - ct_cash_flows) AS MONEY) AS diff
FROM final
