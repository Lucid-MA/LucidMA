WITH
balance_history AS (
  SELECT
    *
  FROM {{ ref('stg_lucid__balance_history') }}
),
flows AS (
  SELECT
    *
  FROM {{ ref('cash_tracker__flows_after_recon') }}
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
transactions AS (
  SELECT
    report_date,
    transaction_action_id,
    fund
  FROM flows
  GROUP BY report_date, fund, transaction_action_id
),
cash_flows AS (
  SELECT
    t.report_date,
    t.transaction_action_id,
    COALESCE(main.flow_is_settled,margin.flow_is_settled) AS flow_is_settled,
    t.fund,
    COALESCE(main.transaction_desc,margin.transaction_desc) AS flow_description,
    COALESCE(main.generated_id,margin.generated_id) AS generated_id,
    main.flow_amount AS main_cash_amount,
    margin.flow_amount AS margin_cash_amount
  FROM transactions AS t
  LEFT JOIN flows AS main ON (
    t.report_date=main.report_date 
    AND t.transaction_action_id=main.transaction_action_id
    AND main.flow_account = 'MAIN'
    )
  LEFT JOIN flows AS margin ON (
    t.report_date=margin.report_date 
    AND t.transaction_action_id=margin.transaction_action_id
    AND margin.flow_account = 'MARGIN'
    )
),
unsettled_flows AS (
  SELECT
    report_date,
    transaction_action_id,
    flow_is_settled,
    fund,
    CONCAT(
      CASE
        WHEN flow_is_settled IS NULL THEN 'UNSETTLED: '
        ELSE 'FAILING: '
      END,
      transaction_desc
    ) AS flow_description,
    generated_id * 10 AS generated_id,
    CASE
      WHEN flow_account = 'MAIN' THEN -flow_amount
      ELSE 0
    END AS main_cash_amount,
    CASE
      WHEN flow_account = 'MARGIN' THEN -flow_amount
      ELSE 0
    END AS margin_cash_amount
  FROM flows
  WHERE flow_is_settled IS NULL OR flow_is_settled = 0
),
combined_flows AS (
  SELECT
    *
  FROM cash_flows
  UNION ALL
  SELECT
    *
  FROM unsettled_flows
),
bod_balance_rows AS (
  SELECT
    report_date,
    fund,
    'Begining balance' AS flow_description,
    0 AS generated_id,
    0 AS main_cash_amount,
    MAX(CASE WHEN flow_account = 'MAIN' AND flow_security = '{{var('CASH')}}' AND flow_status = '{{var('AVAILABLE')}}' THEN bod_balance END) AS main_cash_balance,
    MAX(CASE WHEN flow_account = 'MAIN' AND flow_security = '{{var('SWEEP')}}' AND flow_status = '{{var('AVAILABLE')}}' THEN bod_balance END) AS main_sweep_balance,
    0 AS margin_cash_amount,
    MAX(CASE WHEN flow_account = 'MARGIN' AND flow_security = '{{var('CASH')}}' AND flow_status = '{{var('AVAILABLE')}}' THEN bod_balance END) AS margin_cash_balance,
    MAX(CASE WHEN flow_account = 'MARGIN' AND flow_security = '{{var('SWEEP')}}' AND flow_status = '{{var('AVAILABLE')}}' THEN bod_balance END) AS margin_sweep_balance
  FROM bod_balance
  GROUP BY report_date, fund, series
),
final AS (
  SELECT
    report_date,
    NULL AS flow_is_settled,
    fund,
    flow_description,
    generated_id,
    main_cash_amount,
    main_cash_balance,
    main_sweep_balance,
    margin_cash_amount,
    margin_cash_balance,
    margin_sweep_balance
  FROM bod_balance_rows
  UNION ALL
  SELECT
    cf.report_date,
    cf.flow_is_settled,
    cf.fund,
    cf.flow_description,
    cf.generated_id,
    COALESCE(cf.main_cash_amount, 0) AS main_cash_amount,
    COALESCE(SUM(cf.main_cash_amount) OVER (PARTITION BY cf.report_date, cf.fund ORDER BY cf.generated_id),0) + main.bod_balance AS main_cash_balance,
    smain.bod_balance AS main_sweep_balance,
    COALESCE(cf.margin_cash_amount, 0) AS margin_cash_amount,
    COALESCE(SUM(cf.margin_cash_amount) OVER (PARTITION BY cf.report_date, cf.fund ORDER BY cf.generated_id),0) + margin.bod_balance AS margin_cash_balance,
    smargin.bod_balance AS margin_sweep_balance
  FROM combined_flows AS cf
  LEFT JOIN bod_balance AS main ON (
    cf.report_date = main.report_date
    AND cf.fund = main.fund
    AND main.series = ''
    AND main.flow_account = 'MAIN'
    AND main.flow_security = '{{var('CASH')}}'
    AND main.flow_status = '{{var('AVAILABLE')}}'
  )
  LEFT JOIN bod_balance AS smain ON (
    cf.report_date = smain.report_date
    AND cf.fund = smain.fund
    AND smain.series = ''
    AND smain.flow_account = 'MAIN'
    AND smain.flow_security = '{{var('SWEEP')}}'
    AND smain.flow_status = '{{var('AVAILABLE')}}'
  )
  LEFT JOIN bod_balance AS margin ON (
    cf.report_date = margin.report_date
    AND cf.fund = margin.fund
    AND margin.series = ''
    AND margin.flow_account = 'MARGIN'
    AND margin.flow_security = '{{var('CASH')}}'
    AND margin.flow_status = '{{var('AVAILABLE')}}'
  )
  LEFT JOIN bod_balance AS smargin ON (
    cf.report_date = smargin.report_date
    AND cf.fund = smargin.fund
    AND smargin.series = ''
    AND smargin.flow_account = 'MARGIN'
    AND smargin.flow_security = '{{var('SWEEP')}}'
    AND smargin.flow_status = '{{var('AVAILABLE')}}'
  )
)

SELECT * FROM final