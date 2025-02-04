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
    cash_posting_transaction_timestamp,
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
    local_amount,
    transaction_type_name,
    ct_use,
    CASE 
      WHEN flow_amount > 0 AND flow_after_sweep = 1 AND transaction_type_name = 'CASH DEPOSIT' THEN SUM(CASE WHEN flow_amount > 0 AND flow_after_sweep = 1 AND transaction_type_name = 'CASH DEPOSIT' THEN flow_amount ELSE 0 END) OVER (PARTITION BY report_date, fund, flow_account)
      ELSE 0
    END AS sum_cash_deposit_flows,
    CASE
      WHEN flow_amount > 0 AND flow_after_sweep = 1 AND transaction_type_name = 'CASH DEPOSIT' THEN SUM(CASE WHEN flow_amount > 0 AND flow_after_sweep = 1 AND transaction_type_name = 'CASH DEPOSIT' THEN local_amount ELSE 0 END) OVER (PARTITION BY report_date, fund, flow_account) 
      ELSE 0
    END AS sum_cash_deposit_local,
    CASE 
      WHEN flow_amount < 0 AND flow_after_sweep = 1 AND transaction_type_name = 'BUY' THEN SUM(CASE WHEN flow_amount < 0 AND flow_after_sweep = 1 AND transaction_type_name = 'BUY' THEN flow_amount ELSE 0 END) OVER (PARTITION BY report_date, fund, flow_account)
      ELSE 0
    END AS sum_buy_flows,
     CASE 
      WHEN flow_amount < 0 AND flow_after_sweep = 1 AND transaction_type_name = 'BUY' THEN SUM(CASE WHEN flow_amount < 0 AND flow_after_sweep = 1 AND transaction_type_name = 'BUY' THEN local_amount ELSE 0 END) OVER (PARTITION BY report_date, fund, flow_account)
      ELSE 0
    END AS sum_buy_local
  FROM flows
)

SELECT * FROM final
