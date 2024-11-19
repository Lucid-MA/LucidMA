WITH
flows AS (
  SELECT
    *
  FROM {{ ref('cash_tracker__flows_after_force_failing') }}
  --FROM {{ ref('cash_tracker__flows_after_recon') }}
),
balance_history AS (
  SELECT
    *
  FROM {{ ref('stg_lucid__balance_history') }}
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
funds_to_sweep AS (
  SELECT
    DISTINCT
    c.report_date,
    a.fund,
    a.acct_name AS flow_account,
    a.acct_number AS flow_acct_number
  FROM {{ ref('stg_lucid__cash_and_security_transactions') }} AS c
  JOIN {{ ref('stg_lucid__accounts') }} AS a
    ON (a.acct_number = c.short_acct_number)
  WHERE c.sweep_detected = 1
    --AND TRIM(UPPER(c.transaction_type_name)) != 'INTERNAL MOVEMENT'
),
eod_flows AS (
  SELECT
    f.*
  FROM flows f
  WHERE 1=1
    AND fund IN (SELECT fund FROM funds_to_sweep WHERE report_date = f.report_date)
    AND series = ''
    AND flow_is_settled = 1
),
eod_positions AS (
  SELECT
    report_date,
    fund,
    flow_account,
    flow_acct_number,
    flow_security,
    flow_status,
    SUM(flow_amount) AS total,
    COUNT(1) AS cnt
  FROM eod_flows
  GROUP BY report_date, fund, flow_account, flow_acct_number, flow_security, flow_status
),
unsettled_flows AS (
  SELECT
    f.*
  FROM flows f
  WHERE 1=1
    AND fund IN (SELECT fund FROM funds_to_sweep WHERE report_date = f.report_date)
    AND f.flow_is_settled = 0
    AND f.is_margin = 0
    AND f.is_hxswing = 0
    AND f.flow_amount < 0
),
unsettled_amounts AS (
  SELECT
    report_date,
    fund,
    flow_account,
    flow_security,
    flow_status,
    SUM(flow_amount) AS total,
    COUNT(1) AS cnt
  FROM unsettled_flows f
  GROUP BY report_date, fund, flow_account, flow_security, flow_status
),
combined AS (
  SELECT
    eod.report_date,
    eod.fund,
    eod.flow_account,
    eod.flow_acct_number,
    eod.flow_security,
    eod.flow_status,
    bod.bod_balance,
    eod.total AS eod_movements,
    un.total AS unsettled_amount,
    (-1 * (COALESCE(bod.bod_balance,0) + COALESCE(eod.total,0))) - (COALESCE(un.total,0)) AS sweep_amount
  FROM eod_positions eod
  LEFT JOIN bod_balance bod ON (eod.report_date=bod.report_date AND eod.fund=bod.fund AND eod.flow_account=bod.flow_account AND eod.flow_security=bod.flow_security AND eod.flow_status=bod.flow_status)
  LEFT JOIN unsettled_amounts un ON (eod.report_date=un.report_date AND eod.fund=un.fund AND eod.flow_account=un.flow_account AND eod.flow_security=un.flow_security AND eod.flow_status=bod.flow_status)
  WHERE 1=1
    AND eod.fund IN (SELECT fund FROM funds_to_sweep WHERE report_date = eod.report_date)
),
final AS (
  SELECT
    report_date,
    fund,
    '' AS series,
    'sweep-cash' AS [route],
    'MMSweepCASH ' + flow_account AS transaction_action_id,
    'MM Sweep' AS transaction_desc,
    flow_account,
    flow_acct_number,
    '{{var('CASH')}}' AS flow_security,
    '{{var('AVAILABLE')}}' flow_status,
    sweep_amount AS flow_amount,
    bod_balance,
    eod_movements,
    unsettled_amount,
    1 AS flow_is_settled,
    0 AS flow_after_sweep,
    NULL AS trade_id,
    NULL AS counterparty,
    1 AS used_alloc,
    NULL AS is_margin,
    NULL AS is_hxswing,
    NULL AS generated_id
  FROM combined
  UNION ALL
   SELECT
    report_date,
    fund,
    '' AS series,
    'sweep-vechicle' AS [route],
    'MMSweepVEH ' + flow_account AS transaction_action_id,
    'MM Sweep' AS transaction_desc,
    flow_account,
    flow_acct_number,
    '{{var('SWEEP')}}' AS flow_security,
    '{{var('AVAILABLE')}}' flow_status,
    -sweep_amount AS flow_amount,
    bod_balance,
    eod_movements,
    unsettled_amount,
    1 AS flow_is_settled,
    0 AS flow_after_sweep,
    NULL AS trade_id,
    NULL AS counterparty,
    1 AS used_alloc,
    NULL AS is_margin,
    NULL AS is_hxswing,
    NULL AS generated_id
  FROM combined
)

SELECT * FROM eod_flows
UNION ALL
SELECT * FROM unsettled_flows
