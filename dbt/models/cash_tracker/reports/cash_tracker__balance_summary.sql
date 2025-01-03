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
      c.calendar_date AS report_date,
      a.fund,
      a.acct_name,
      a.acct_number AS account_number,
      CAST(a.acct_number AS VARCHAR) + '8400' AS cash_account_number
  FROM {{ ref('stg_lucid__accounts') }} AS a
  CROSS JOIN {{ ref('stg_lucid__calendar') }} AS c
  WHERE c.is_business_day = 1
  AND c.calendar_date >= '2024-11-01'
  AND c.calendar_date < CAST(GETDATE() AS DATE)
  --AND cash_account_number LIKE '%8400'
),
bnym_activity AS (
  SELECT
    report_date,
    short_acct_number,
    MAX(sweep_detected) AS sweep_detected,
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
final_settled_flows AS (
  SELECT
    report_date,
    fund,
    flow_account,
    flow_acct_number,
    SUM(flow_amount) AS cash_total
  FROM {{ ref('cash_tracker__flows_after_force_failing') }}
  WHERE flow_is_settled = 1
  GROUP BY report_date, fund, flow_account, flow_acct_number
),
combined AS (
  SELECT
    ah.report_date,
    ah.fund,
    ah.acct_name,
    ah.account_number,
    ba.sweep_detected,
    COALESCE(ch.prior_eod_balance, 0) AS cash_actual_bod,
    COALESCE(ba.cash_amount, 0) AS bnym_cash_activity,
    COALESCE(ba.sweep_amount, 0) * -1 AS bnym_sweep_activity,
    COALESCE(bhc.ending_balance, 0) AS cash_actual_eod,
    COALESCE(sh.prior_eod_balance, 0) AS sweep_actual_bod,
    COALESCE(bhs.ending_balance, 0) AS sweep_actual_eod,
    COALESCE(cf.cash_total, 0) AS ct_cash_flows
  FROM account_history AS ah
  LEFT JOIN eod_history AS ch ON (ah.report_date=ch.report_date AND ah.account_number=ch.account_number AND ch.security LIKE 'CASHUSD%')
  LEFT JOIN eod_history AS sh ON (ah.report_date=sh.report_date AND ah.account_number=sh.account_number AND sh.security LIKE 'X9X9USD%')
  LEFT JOIN bnym_activity AS ba ON (ah.report_date=ba.report_date AND ah.account_number=ba.short_acct_number)
  LEFT JOIN balance_history AS bhc ON (ah.report_date=bhc.report_date AND ah.cash_account_number=bhc.cash_account_number AND bhc.security LIKE 'CASHUSD%')
  LEFT JOIN balance_history AS bhs ON (ah.report_date=bhs.report_date AND ah.cash_account_number=bhs.cash_account_number AND bhs.security LIKE 'X9X9USD%')
  LEFT JOIN final_settled_flows AS cf ON (ah.report_date=cf.report_date AND ah.account_number=cf.flow_acct_number)
),
calc_eod AS (
  SELECT
    report_date,
    fund,
    acct_name,
    account_number,
    cash_actual_eod,
    cash_actual_bod,
    bnym_cash_activity,
    (cash_actual_bod + bnym_cash_activity) AS bnym_cash_eod,
    ct_cash_flows,
    (cash_actual_bod + ct_cash_flows - bnym_sweep_activity) AS ct_cash_eod,
    sweep_actual_eod,
    sweep_actual_bod,
    bnym_sweep_activity,
    (sweep_actual_bod + bnym_sweep_activity) AS bnym_sweep_eod,
    sweep_detected
  FROM combined
),
final AS (
  SELECT 
    report_date,
    fund,
    acct_name,
    account_number,
    cash_actual_bod,
    ct_cash_flows,
    -1 * bnym_sweep_activity AS ct_sweep_activity,
    ct_cash_eod,
    bnym_cash_eod,
    (ct_cash_eod - bnym_cash_eod) as diff_cash_eod,
    sweep_actual_bod,
    bnym_sweep_activity,
    bnym_sweep_eod
  FROM calc_eod
)

SELECT * FROM final