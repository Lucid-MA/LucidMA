WITH master_flows AS (
  SELECT
    *
  FROM
    {{ ref('cash_tracker__flows_after_force_failing') }}
),
series_flows AS (
  SELECT
    *
  FROM
    {{ ref('cash_tracker__flows_series') }}
  WHERE
    flow_security = '{{var('CASH')}}'
    AND flow_status = '{{var('AVAILABLE')}}'
),
accounts AS (
  SELECT
    *
  FROM
    {{ ref('stg_lucid__accounts') }}
),
series AS (
  SELECT
    *
  FROM
    {{ ref('stg_lucid__series') }}
),
combined AS (
  SELECT
    m.report_date,
    m.orig_report_date,
    m.fund,
    s.series,
    m.[_flow_id],
    m.[route],
    m.transaction_action_id,
    m.transaction_desc,
    m.flow_account,
    m.flow_acct_number,
    m.flow_security,
    m.flow_status,
    s.flow_amount,
    m.flow_is_settled,
    m.flow_after_sweep,
    m.trade_id,
    m.counterparty,
    s.used_alloc,
    m.is_margin,
    m.is_hxswing,
    m.sweep_detected,
    m.generated_id,
    m.reference_number,
    m.ct_use
  FROM master_flows AS m
  JOIN series_flows AS s ON (
    m.orig_report_date = s.report_date
    AND m.[_flow_id] = s.[_flow_id]
  )
),
FINAL AS (
  SELECT
    *
  FROM
    combined
)
SELECT
  *
FROM
  FINAL
