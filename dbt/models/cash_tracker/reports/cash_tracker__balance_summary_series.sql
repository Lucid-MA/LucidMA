WITH 
master_summary AS (
  SELECT
    *,
    (ct_sweep_activity + ct_cash_flows + cash_actual_bod) AS ct_sweep_extra
  FROM {{ ref('cash_tracker__balance_summary') }}
  WHERE report_date >= '2024-10-31'
  AND acct_name IN ('MAIN','MARGIN','MANAGEMENT')
),
master_flows AS (
    SELECT
        *
    FROM {{ ref('cash_tracker__flows_after_force_failing') }}
    WHERE ct_use = 1
    AND report_date >= '2024-10-31'
),
series_summary AS (
  SELECT
    *
  FROM {{ ref('stg_lucid__balance_history_series') }}
  WHERE report_date >= '2024-10-31'
  AND flow_account IN ('MAIN','MARGIN','MANAGEMENT')
),
series_flows AS (
    SELECT
        *
    FROM {{ ref('cash_tracker__flows_after_force_failing_series') }}
    WHERE ct_use = 1
    AND report_date >= '2024-10-31'
),
accounts AS (
  SELECT
    *
  FROM {{ ref('stg_lucid__accounts') }}
  WHERE 1=1
  AND acct_name IN ('MAIN','MARGIN','MANAGEMENT')
),
series AS (
  SELECT
    *
  FROM {{ ref('stg_lucid__series') }}
),
series_bod_balance AS (
  SELECT
    {{ next_business_day('report_date') }} AS report_date,
    fund,
    flow_account,
    series,
    TRY_CAST(COALESCE(series_cash_eod, 0) AS MONEY) AS cash_balance,
    TRY_CAST(COALESCE(series_sweep_eod, 0) AS MONEY) AS sweep_balance
  FROM series_summary
),
final_settled_flows AS (
  SELECT
    s.report_date,
    s.fund,
    s.flow_account,
    s.series,
    a.acct_number AS account_number,
    TRY_CAST(SUM(
      CASE
        WHEN s.flow_is_settled = 1 THEN s.flow_amount
        ELSE 0
      END
    ) AS MONEY) AS series_settled_activity,
    TRY_CAST(SUM(
      CASE
        WHEN s.flow_is_settled = 0 THEN s.flow_amount
        ELSE 0
      END
    ) AS MONEY) AS series_unsettled_activity,
    TRY_CAST(SUM(cash_deposit_amt) AS MONEY) AS series_cash_deposit,
    TRY_CAST(SUM(revrepo_open_amt) AS MONEY) AS series_revrepo_open
  FROM series_flows AS s
  JOIN accounts AS a ON (s.fund = a.fund AND s.flow_account = a.acct_name)
  GROUP BY s.report_date, s.fund, s.flow_account, s.series, a.acct_number
),
series_combined AS (
  SELECT
    TRY_CAST(
      m.report_date AS DATE
    ) AS report_date,
    m.fund,
    m.acct_name AS flow_account,
    s.series,
    m.account_number,
    COALESCE(p.series_settled_activity, 0) AS series_settled_activity,
    COALESCE(p.series_unsettled_activity, 0) AS series_unsettled_activity,
    COALESCE(bod.cash_balance, 0) AS series_cash_bod,
    COALESCE(bod.sweep_balance, 0) AS series_sweep_bod,
    CAST(
      SUM(p.series_settled_activity) OVER (PARTITION BY m.report_date, m.fund, m.account_number) 
    AS MONEY) AS series_flows_total,
    (bod.cash_balance+bod.sweep_balance) AS series_acct_total,
    CAST(
      SUM((bod.cash_balance+bod.sweep_balance)) OVER (PARTITION BY m.report_date, m.fund, m.account_number) 
    AS MONEY) AS series_sum_total,
    (m.cash_actual_bod+m.sweep_actual_bod) AS master_acct_total,
    CAST(m.ct_cash_flows AS MONEY) AS ct_cash_flows,
    CAST(m.bnym_cash_activity AS MONEY) AS bnym_cash_activity,
    CAST(m.ct_sweep_activity AS MONEY) AS ct_sweep_activity,
    (m.ct_cash_flows+m.cash_actual_bod) AS ct_cash_total,
    m.ct_sweep_extra,
    COALESCE(p.series_cash_deposit, 0) AS series_cash_deposit,
    CAST(
      SUM(p.series_cash_deposit) OVER (PARTITION BY m.report_date, m.fund, m.account_number) 
    AS MONEY) AS series_cash_deposit_total,
    COALESCE(p.series_revrepo_open, 0) AS series_revrepo_open,
    CAST(
      SUM(p.series_revrepo_open) OVER (PARTITION BY m.report_date, m.fund, m.account_number) 
    AS MONEY) AS series_revrepo_open_total
  FROM master_summary AS m 
  JOIN series AS s ON (m.fund = s.fund)
  LEFT JOIN final_settled_flows AS p ON (
    m.report_date = p.report_date
    AND m.fund = p.fund
    AND m.account_number = p.account_number
    AND s.series = p.series
  )
  LEFT JOIN series_bod_balance AS bod ON (
    m.report_date = bod.report_date
    AND m.fund = bod.fund
    AND m.acct_name = bod.flow_account
    AND s.series = bod.series
  )
),
series_combined_final AS (
  SELECT
    *,
    - (series_cash_bod + series_settled_activity) AS base_sweep_amt,
    COALESCE((series_acct_total/NULLIF(series_sum_total,0)), 0) AS series_total_ratio
  FROM series_combined
),
calc_sweep_cash_deposit AS (
  SELECT
    *,
    CASE
      WHEN abs(ct_sweep_extra) > 0.01 AND SIGN(ct_sweep_extra) = SIGN(series_cash_deposit_total) AND series_cash_deposit_total > ct_sweep_extra THEN COALESCE((series_cash_deposit/NULLIF(series_cash_deposit_total,0)), 0) * ct_sweep_extra
      WHEN abs(ct_sweep_extra) > 0.01 AND series_cash_deposit_total > 0 THEN series_cash_deposit
      ELSE 0
    END AS cash_deposit_sweep_amt,
    CASE
      WHEN abs(ct_sweep_extra) > 0.01 AND SIGN(ct_sweep_extra) = SIGN(series_cash_deposit_total) AND series_cash_deposit_total > ct_sweep_extra THEN 0
      WHEN abs(ct_sweep_extra) > 0.01 AND series_cash_deposit_total > 0 THEN ct_sweep_extra - series_cash_deposit_total 
      ELSE ct_sweep_extra
    END AS ct_sweep_extra_1
  FROM series_combined_final
),
calc_sweep_revrepo AS (
  SELECT
    *,
    CASE
      WHEN abs(ct_sweep_extra_1) > 0.01 AND SIGN(ct_sweep_extra_1) = SIGN(series_revrepo_open_total) AND series_revrepo_open_total > ct_sweep_extra_1 THEN COALESCE((series_revrepo_open/NULLIF(series_revrepo_open_total,0)),0) * ct_sweep_extra_1
      WHEN abs(ct_sweep_extra_1) > 0.01 AND series_revrepo_open_total < 0 THEN series_revrepo_open
      ELSE 0
    END AS revrepo_sweep_amt,
    CASE
      WHEN abs(ct_sweep_extra_1) > 0.01 AND SIGN(ct_sweep_extra_1) = SIGN(series_revrepo_open_total) AND series_revrepo_open_total > ct_sweep_extra_1 THEN 0
      WHEN abs(ct_sweep_extra_1) > 0.01 AND series_revrepo_open_total < 0 THEN ct_sweep_extra_1 - series_revrepo_open_total
      ELSE ct_sweep_extra_1
    END AS ct_sweep_extra_2
  FROM calc_sweep_cash_deposit
),
calc_sweep_default AS (
  SELECT
    *,
    CASE
      WHEN abs(ct_sweep_extra_2) > 0.01 THEN (series_total_ratio * ct_sweep_extra_2)
      ELSE 0
    END AS extra_sweep_amt
  FROM calc_sweep_revrepo
),
calc_sweep_amt AS (
  SELECT
    *,
    (base_sweep_amt + cash_deposit_sweep_amt + revrepo_sweep_amt + extra_sweep_amt) AS series_sweep_amount
  FROM calc_sweep_default
),
final AS (
  SELECT
    report_date,
    fund,
    flow_account,
    series,
    account_number,
    series_settled_activity,
    series_unsettled_activity,
    series_cash_bod,
    series_sweep_bod
    series_acct_total,
    series_sum_total,
    master_acct_total,
    ct_cash_flows,
    bnym_cash_activity,
    ct_sweep_activity,
    series_cash_deposit,
    series_cash_deposit_total,
    series_revrepo_open,
    series_revrepo_open_total,
    series_total_ratio,
    base_sweep_amt,
    ct_sweep_extra,
    cash_deposit_sweep_amt,
    ct_sweep_extra_1,
    revrepo_sweep_amt,
    ct_sweep_extra_2,
    extra_sweep_amt,
    series_sweep_amount
  FROM calc_sweep_amt
)

SELECT * FROM final
