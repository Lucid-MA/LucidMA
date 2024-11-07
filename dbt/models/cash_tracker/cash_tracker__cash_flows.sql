WITH
balance_history AS (
  SELECT
    *
  FROM {{ ref('stg_lucid__balance_history') }}
),
flows AS (
  SELECT
    report_date,
    fund,
    series,
    transaction_action_id,
    transaction_desc,
    flow_account,
    flow_acct_number,
    CASE
      WHEN flow_is_settled = 1 THEN flow_amount
      ELSE NULL
    END AS flow_amount,
    flow_security,
    flow_status,
    flow_is_settled,
    flow_after_sweep,
    trade_id,
    counterparty,
    used_alloc,
    is_hxswing,
    is_margin,
    generated_id
  FROM {{ ref('cash_tracker__flows_after_force_failing') }}
  WHERE 
    series = ''
    AND flow_security = '{{var('CASH')}}'
    AND flow_status = '{{var('AVAILABLE')}}'
),
cash_accounts AS (
  SELECT
    DISTINCT
    f.report_date,
    bh.fund,
    bh.series,
    bh.acct_name AS flow_account,
    bh.security AS flow_security,
    bh.status AS flow_status
  FROM flows AS f
  JOIN balance_history AS bh ON (1=1)
),
bod_balance AS (
  SELECT
    DISTINCT
      ca.report_date,
      ca.fund,
      ca.series,
      ca.flow_account,
      ca.flow_security,
      ca.flow_status,
      bh.amount AS bod_balance
  FROM cash_accounts AS ca 
  CROSS APPLY (
    SELECT TOP 1 bh.amount
    FROM balance_history AS bh
    WHERE bh.fund = ca.fund
      AND bh.series = ca.series
      AND bh.acct_name = ca.flow_account
      AND bh.security = ca.flow_security
      AND bh.status = ca.flow_status
      AND bh.balance_date < ca.report_date
    ORDER BY bh.balance_date DESC
  ) AS bh
),
sweeps AS (
  SELECT
    report_date,
    fund,
    series,
    transaction_action_id,
    transaction_desc,
    flow_account,
    flow_acct_number,
    flow_amount,
    flow_security,
    flow_status,
    flow_is_settled,
    flow_after_sweep,
    trade_id,
    counterparty,
    used_alloc,
    is_hxswing,
    is_margin,
    generated_id
  FROM {{ ref('cash_tracker__sweeps') }}
  WHERE
    series = ''
    AND flow_security = '{{var('CASH')}}'
    AND flow_status = '{{var('AVAILABLE')}}'
),
sweeps2 AS (
  SELECT
    report_date,
    transaction_desc,
    fund,
    series
  FROM sweeps
  GROUP BY report_date, fund, series, transaction_desc
),
sweep_flows AS (
  SELECT
    t.report_date,
    t.transaction_desc AS transaction_action_id,
    COALESCE(main.flow_is_settled,expense.flow_is_settled,margin.flow_is_settled,mgmt.flow_is_settled,subscription.flow_is_settled) AS flow_is_settled,
    t.fund,
    t.series,
    t.transaction_desc AS flow_description,
    COALESCE(main.generated_id,expense.generated_id,margin.generated_id,mgmt.generated_id,subscription.generated_id) AS generated_id,
    main.flow_amount AS main_cash_amount,
    expense.flow_amount AS expense_cash_amount,
    margin.flow_amount AS margin_cash_amount,
    mgmt.flow_amount AS mgmt_cash_amount,
    subscription.flow_amount AS subscription_cash_amount
  FROM sweeps2 AS t
  LEFT JOIN sweeps AS main ON (
    t.report_date=main.report_date 
    AND t.transaction_desc=main.transaction_desc
    AND t.fund=main.fund
    AND t.series=main.series
    AND main.flow_account = 'MAIN'
    )
  LEFT JOIN sweeps AS expense ON (
    t.report_date=expense.report_date 
    AND t.transaction_desc=expense.transaction_desc
    AND t.fund=expense.fund
    AND t.series=expense.series
    AND expense.flow_account = 'EXPENSE'
    )
  LEFT JOIN sweeps AS margin ON (
    t.report_date=margin.report_date 
    AND t.transaction_desc=margin.transaction_desc
    AND t.fund=margin.fund
    AND t.series=margin.series
    AND margin.flow_account = 'MARGIN'
    )
  LEFT JOIN sweeps AS mgmt ON (
    t.report_date=mgmt.report_date 
    AND t.transaction_desc=mgmt.transaction_desc
    AND t.fund=mgmt.fund
    AND t.series=mgmt.series
    AND mgmt.flow_account = 'MANAGEMENT'
    )
  LEFT JOIN sweeps AS subscription ON (
    t.report_date=subscription.report_date 
    AND t.transaction_desc=subscription.transaction_desc
    AND t.fund=subscription.fund
    AND t.series=subscription.series
    AND subscription.flow_account = 'SUBSCRIPTION'
    )
),
transactions AS (
  SELECT
    report_date,
    transaction_action_id,
    transaction_desc,
    fund,
    series
  FROM flows
  GROUP BY report_date, transaction_action_id, fund, series, transaction_desc
),
cash_flows AS (
  SELECT
    t.report_date,
    t.transaction_action_id,
    COALESCE(main.flow_is_settled,expense.flow_is_settled,margin.flow_is_settled,mgmt.flow_is_settled,subscription.flow_is_settled) AS flow_is_settled,
    t.fund,
    t.series,
    t.transaction_desc AS flow_description,
    COALESCE(main.generated_id,expense.generated_id,margin.generated_id,mgmt.generated_id,subscription.generated_id) AS generated_id,
    main.flow_amount AS main_cash_amount,
    expense.flow_amount AS expense_cash_amount,
    margin.flow_amount AS margin_cash_amount,
    mgmt.flow_amount AS mgmt_cash_amount,
    subscription.flow_amount AS subscription_cash_amount
  FROM transactions AS t
  LEFT JOIN flows AS main ON (
    t.report_date=main.report_date 
    AND t.transaction_action_id=main.transaction_action_id
    AND t.fund=main.fund
    AND t.series=main.series
    AND main.flow_account = 'MAIN'
    )
  LEFT JOIN flows AS expense ON (
    t.report_date=expense.report_date 
    AND t.transaction_action_id=expense.transaction_action_id
    AND t.fund=expense.fund
    AND t.series=expense.series
    AND expense.flow_account = 'EXPENSE'
    )
  LEFT JOIN flows AS margin ON (
    t.report_date=margin.report_date 
    AND t.transaction_action_id=margin.transaction_action_id
    AND t.fund=margin.fund
    AND t.series=margin.series
    AND margin.flow_account = 'MARGIN'
    )
  LEFT JOIN flows AS mgmt ON (
    t.report_date=mgmt.report_date 
    AND t.transaction_action_id=mgmt.transaction_action_id
    AND t.fund=mgmt.fund
    AND t.series=mgmt.series
    AND mgmt.flow_account = 'MANAGEMENT'
    )
  LEFT JOIN flows AS subscription ON (
    t.report_date=subscription.report_date 
    AND t.transaction_action_id=subscription.transaction_action_id
    AND t.fund=subscription.fund
    AND t.series=subscription.series
    AND subscription.flow_account = 'SUBSCRIPTION'
    )
),
all_flows AS (
 SELECT
    report_date,
    transaction_action_id,
    flow_is_settled,
    fund,
    series,
    flow_description,
    generated_id,
    main_cash_amount,
    expense_cash_amount,
    margin_cash_amount,
    mgmt_cash_amount,
    subscription_cash_amount
  FROM cash_flows
  UNION ALL
   SELECT
    report_date,
    transaction_action_id,
    flow_is_settled,
    fund,
    series,
    flow_description,
    generated_id,
    main_cash_amount,
    expense_cash_amount,
    margin_cash_amount,
    mgmt_cash_amount,
    subscription_cash_amount
  FROM sweep_flows
),
final AS (
  SELECT
    cf.report_date,
    cf.flow_is_settled,
    cf.fund,
    cf.series,
    cf.generated_id,
    COALESCE(cf.main_cash_amount, 0) AS main_cash_flow,
    COALESCE(SUM(cf.main_cash_amount) OVER (PARTITION BY cf.report_date, cf.fund, cf.series ORDER BY cf.generated_id),0) + main.bod_balance AS main_balance,
    COALESCE(cf.expense_cash_amount, 0) AS expense_cash_flow,
    COALESCE(SUM(cf.expense_cash_amount) OVER (PARTITION BY cf.report_date, cf.fund, cf.series ORDER BY cf.generated_id),0) + expense.bod_balance AS expense_balance,
    COALESCE(cf.margin_cash_amount, 0) AS margin_cash_flow,
    COALESCE(SUM(cf.margin_cash_amount) OVER (PARTITION BY cf.report_date, cf.fund, cf.series ORDER BY cf.generated_id),0) + margin.bod_balance AS margin_balance,
    COALESCE(cf.mgmt_cash_amount, 0) AS mgmt_cash_flow,
    COALESCE(SUM(cf.mgmt_cash_amount) OVER (PARTITION BY cf.report_date, cf.fund, cf.series ORDER BY cf.generated_id),0) + mgmt.bod_balance AS mgmt_balance,
    COALESCE(cf.subscription_cash_amount, 0) AS subscription_cash_flow,
    COALESCE(SUM(cf.subscription_cash_amount) OVER (PARTITION BY cf.report_date, cf.fund, cf.series ORDER BY cf.generated_id),0) + subscription.bod_balance AS subscription_balance,
    cf.flow_description
  FROM all_flows AS cf
  LEFT JOIN bod_balance AS main ON (
    cf.report_date = main.report_date
    AND cf.fund = main.fund
    AND main.series = ''
    AND main.flow_account = 'MAIN'
    AND main.flow_security = '{{var('CASH')}}'
    AND main.flow_status = '{{var('AVAILABLE')}}'
  )
  LEFT JOIN bod_balance AS expense ON (
    cf.report_date = expense.report_date
    AND cf.fund = expense.fund
    AND expense.series = ''
    AND expense.flow_account = 'EXPENSE'
    AND expense.flow_security = '{{var('SWEEP')}}'
    AND expense.flow_status = '{{var('AVAILABLE')}}'
  )
  LEFT JOIN bod_balance AS margin ON (
    cf.report_date = margin.report_date
    AND cf.fund = margin.fund
    AND margin.series = ''
    AND margin.flow_account = 'MARGIN'
    AND margin.flow_security = '{{var('CASH')}}'
    AND margin.flow_status = '{{var('AVAILABLE')}}'
  )
  LEFT JOIN bod_balance AS mgmt ON (
    cf.report_date = mgmt.report_date
    AND cf.fund = mgmt.fund
    AND mgmt.series = ''
    AND mgmt.flow_account = 'MARGIN'
    AND mgmt.flow_security = '{{var('SWEEP')}}'
    AND mgmt.flow_status = '{{var('AVAILABLE')}}'
  )
  LEFT JOIN bod_balance AS subscription ON (
    cf.report_date = subscription.report_date
    AND cf.fund = subscription.fund
    AND subscription.series = ''
    AND subscription.flow_account = 'MARGIN'
    AND subscription.flow_security = '{{var('SWEEP')}}'
    AND subscription.flow_status = '{{var('AVAILABLE')}}'
  )
)

SELECT * FROM final