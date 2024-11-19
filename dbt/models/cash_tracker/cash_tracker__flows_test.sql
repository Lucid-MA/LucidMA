WITH
cash_flows AS (
  SELECT
    CASE
      WHEN main_cash_flow <> 0 THEN 1
      --WHEN expense_cash_flow <> 0 THEN 1
      --WHEN margin_cash_flow <> 0 THEN 1
      --WHEN management_cash_flow <> 0 THEN 1
      --WHEN subscription_cash_flow <> 0 THEN 1
      ELSE 0
    END AS flow_is_settled,
    *
  FROM dbt.cash_flows_prime_master
  WHERE TRIM([description]) != 'MM Sweep'
),
cash_sweeps AS (
  SELECT
    [date],
    COALESCE(MAX(CASE WHEN main_cash_flow != 0 THEN main_cash_flow ELSE NULL END), 0) AS main_cash_flow,
    COALESCE(MAX(CASE WHEN expense_cash_flow != 0 THEN expense_cash_flow ELSE NULL END), 0) AS expense_cash_flow,
    COALESCE(MAX(CASE WHEN margin_cash_flow != 0 THEN margin_cash_flow ELSE NULL END), 0) AS margin_cash_flow,
    COALESCE(MAX(CASE WHEN management_cash_flow != 0 THEN management_cash_flow ELSE NULL END), 0) AS management_cash_flow,
    COALESCE(MAX(CASE WHEN subscription_cash_flow != 0 THEN subscription_cash_flow ELSE NULL END), 0) AS subscription_cash_flow,
    [description]
  FROM dbt.cash_flows_prime_master
  WHERE [description] = 'MM Sweep'
  GROUP BY [date], [description]
),
cash_flows_file AS (
select
  CAST([date] AS date) as report_date,
  flow_is_settled,
  0 AS is_sweep,
  'PRIME' AS fund,
  main_cash_flow,
  --main_balance,
  expense_cash_flow,
  --expense_balance,
  margin_cash_flow,
  --margin_balance,
  management_cash_flow AS mgmt_cash_flow,
  --management_balance AS mgmt_balance,
  subscription_cash_flow,
  --subscription_balance,
  TRIM([description]) AS flow_description
from cash_flows
where 1=1
AND date IN ('2024-09-17','2024-09-18','2024-09-19','2024-09-20','2024-09-23','2024-10-16','2024-10-17')
UNION ALL
select
  CAST([date] AS date) as report_date,
  1 AS flow_is_settled,
  1 AS is_sweep,
  'PRIME' AS fund,
  main_cash_flow,
  expense_cash_flow,
  margin_cash_flow,
  management_cash_flow AS mgmt_cash_flow,
  subscription_cash_flow,
  [description] AS flow_description
from cash_sweeps
where 1=1
AND date IN ('2024-09-17','2024-09-18','2024-09-19','2024-09-20','2024-09-23','2024-10-16','2024-10-17')
),
expected_flows AS (
SELECT 
  report_date,
  reference_number,
  CASE
    WHEN main_cash_flow = 0 THEN 0
    ELSE flow_is_settled
  END AS flow_is_settled,
  CASE
    WHEN flow_description = 'MM Sweep' THEN 1
    ELSE 0
  END AS is_sweep,
  fund,
  main_cash_flow,
  --main_balance,
  expense_cash_flow,
  --expense_balance,
  margin_cash_flow,
  --margin_balance,
  mgmt_cash_flow,
  --mgmt_balance,
  subscription_cash_flow,
  --subscription_balance,
  flow_description
FROM {{ ref('cash_tracker__cash_flows') }}
WHERE 1=1 
AND report_date IN ('2024-10-25','2024-10-17')
),
recon_sweeps_dates AS (
  select 
    report_date,
    fund 
  from {{ ref('cash_tracker__cash_recon_sweeps') }}
  group by report_date, fund
),
recon_sweeps AS (
  select
    *
  from {{ ref('cash_tracker__cash_recon_sweeps') }}
),
observed_sweeps AS (
  select 
    s.report_date,
    1 AS flow_is_settled,
    'BNYM' AS source,
    2 AS is_sweep,
    s.fund,
    main.sweep_amount AS main_cash_flow,
    expense.sweep_amount AS expense_cash_flow,
    margin.sweep_amount AS margin_cash_flow,
    mgmt.sweep_amount AS mgmt_cash_flow,
    sub.sweep_amount AS subscription_cash_flow,
    'BNYM Sweep' AS flow_description
  from recon_sweeps_dates AS s
  LEFT JOIN recon_sweeps AS main ON (
    s.report_date = main.report_date
    AND s.fund = main.fund
    AND main.acct_name = 'MAIN'
  )
  LEFT JOIN recon_sweeps AS expense ON (
    s.report_date = expense.report_date
    AND s.fund = expense.fund
    AND expense.acct_name = 'EXPENSE'
  )
  LEFT JOIN recon_sweeps AS margin ON (
    s.report_date = margin.report_date
    AND s.fund = margin.fund
    AND margin.acct_name = 'MARGIN'
  )
  LEFT JOIN recon_sweeps AS mgmt ON (
    s.report_date = mgmt.report_date
    AND s.fund = mgmt.fund
    AND mgmt.acct_name = 'MANAGEMENT'
  )
  LEFT JOIN recon_sweeps AS sub ON (
    s.report_date = sub.report_date
    AND s.fund = sub.fund
    AND sub.acct_name = 'SUBSCRIPTION'
  )
  WHERE 1=1 
  AND s.report_date IN ('2024-10-25','2024-10-17')
),
combined AS (
  SELECT
    report_date,
    NULL AS reference_number,
    flow_is_settled,
    'JAVA' AS source,
    is_sweep,
    fund,
    main_cash_flow,
    expense_cash_flow,
    margin_cash_flow,
    mgmt_cash_flow,
    subscription_cash_flow,
    flow_description
  FROM cash_flows_file
  UNION ALL
  SELECT
    report_date,
    reference_number,
    flow_is_settled,
    'SQL' AS source,
    is_sweep,
    fund,
    main_cash_flow,
    expense_cash_flow,
    margin_cash_flow,
    mgmt_cash_flow,
    subscription_cash_flow,
    flow_description
  FROM expected_flows
  UNION ALL
  SELECT
    report_date,
    NULL AS reference_number,
    flow_is_settled,
    source,
    is_sweep,
    fund,
    main_cash_flow,
    expense_cash_flow,
    margin_cash_flow,
    mgmt_cash_flow,
    subscription_cash_flow,
    flow_description
  FROM observed_sweeps
),
final AS (
  SELECT
    *
  FROM combined
)

SELECT * FROM final