WITH
flows AS (
  SELECT
    *
  FROM {{ ref('cash_tracker__flows_after_recon') }}
),
final AS (
  SELECT
    report_date,
    orig_report_date,
    fund,
    series,
    [_flow_id],
    [route],
    transaction_action_id,
    transaction_desc,
    flow_account,
    flow_acct_number,
    flow_security,
    flow_status,
    flow_amount,
    CASE
      WHEN flow_is_settled IS NULL AND flow_security = '{{var('CASH')}}' AND flow_status = '{{var('AVAILABLE')}}'
        THEN 0 --failing
      ELSE flow_is_settled
    END AS flow_is_settled,
    flow_after_sweep,
    trade_id,
    counterparty,
    used_alloc,
    is_margin,
    is_hxswing,
    sweep_detected,
    generated_id,
    reference_number,
    ct_use
  FROM flows
)

SELECT * FROM final
