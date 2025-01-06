 {{
    config({
        "as_columnstore": false,
        "materialized": 'table',
        "post-hook": [
            "{{ create_nonclustered_index(columns = ['report_date']) }}",
            "{{ create_nonclustered_index(columns = ['_flow_id']) }}",
            "{{ create_nonclustered_index(columns = ['fund']) }}",
            "{{ create_nonclustered_index(columns = ['trade_id']) }}",
        ]
    })
}}

WITH
cash_recon AS (
  SELECT
    *
  FROM {{ ref('stg_lucid__cash_and_security_transactions') }}
  WHERE 1=1
    AND fund IS NOT NULL
    AND report_date <= CAST(getdate() AS DATE)
    AND (location_name != 'STIF LOCATIONS' OR transaction_type_name = 'DIVIDEND')
    --AND TRIM(UPPER(transaction_type_name)) != 'INTERNAL MOVEMENT'
    --AND cusip_cins != '{{var('SWEEP')}}'
),
cash_flows AS (
  SELECT
    f.*,
    f.acct_number AS flow_acct_number
  FROM {{ ref('cash_tracker__flows_plus_failing_trades') }} AS f
  WHERE 1=1
    AND report_date <= CAST(getdate() AS DATE)
    AND flow_security = '{{ var('CASH') }}'
    AND flow_status = '{{ var('AVAILABLE') }}'
    AND series = ''
    AND SUBSTRING(transaction_action_id,1,8) != 'REALLOC_'
    --AND trade_id = 185391
),
cp_margins AS (
  SELECT
    report_date,
    fund,
    series,
    counterparty,
    SUM(flow_amount) AS margin_total
  FROM cash_flows
  WHERE is_margin = 1
  GROUP BY report_date, fund, series, counterparty
),
expected AS (
   SELECT 
    cash_flows._flow_id,
    CASE
      WHEN flow_amount <= 0 THEN flow_acct_number
      ELSE NULL
    END AS [from],
    CASE
      WHEN flow_amount > 0 THEN flow_acct_number
      ELSE NULL
    END AS [to],
    ABS(flow_amount) AS amount,
    CASE WHEN UPPER(SUBSTRING(transaction_desc,1,7)) = 'HXSWING' THEN 1 ELSE 0 END AS is_hxswing,
    trade_id AS related_helix_id,
    transaction_desc AS [description],
    cash_flows.report_date,
    cash_flows.orig_report_date,
    REPLACE(transaction_desc,'_',' ') AS desc_replaced,
    generated_id,
    cash_flows.fund,
    cash_flows.series,
    [route],
    transaction_action_id,
    transaction_desc,
    flow_account,
    flow_acct_number,
    flow_security,
    flow_status,
    flow_amount,
    flow_is_settled,
    flow_after_sweep,
    trade_id,
    cash_flows.counterparty,
    is_margin,
    CASE WHEN UPPER(SUBSTRING(transaction_desc,1,2)) = 'PO' THEN 1 ELSE 0 END AS is_po,
    cp_margins.margin_total,
    used_alloc
  FROM cash_flows
  LEFT JOIN cp_margins ON (
    cash_flows.fund = cp_margins.fund
    AND cash_flows.series = cp_margins.series
    AND cash_flows.counterparty = cp_margins.counterparty
    AND cash_flows.report_date = cp_margins.report_date
    AND cash_flows.is_margin = 1
    )
),
ranked_matches AS (
  SELECT
    o.client_reference_number,
    o.transaction_type_name,
    e.*,
    o.local_amount,
    o.reference_number,
    o.helix_id AS ob_helix_id,
    o.sweep_detected,
    CASE
      WHEN e.is_margin = 1 AND o.is_hxswing = 0 AND e.flow_amount < 0 AND PATINDEX(UPPER('MRGN '+e.counterparty+'%'), UPPER(o.client_reference_number)) = 1 THEN 2
      WHEN e.is_margin = 1 AND o.is_hxswing = 0 AND e.flow_amount > 0 AND PATINDEX(UPPER('MRGN '+e.counterparty+'%'), UPPER(o.client_reference_number)) = 1 THEN 3
      WHEN e.is_margin = 1 AND o.is_hxswing = 0 AND e.margin_total > 0 AND o.transaction_type_name = 'CASH DEPOSIT' AND {{ abs_diff('o.local_amount', 'e.margin_total') }} <= 0.05 THEN 4 + {{ abs_diff('o.local_amount', 'e.margin_total') }}
      WHEN e.is_margin = 1 AND o.is_hxswing = 0 AND e.margin_total <= 0 AND {{ abs_diff('o.local_amount', 'e.margin_total') }} <= 0.05 THEN 5 + {{ abs_diff('o.local_amount', 'e.margin_total') }}
      WHEN e.is_margin = 1 AND o.is_hxswing = 0 AND e.flow_amount < 0 AND {{ abs_diff('o.local_amount', 'e.flow_amount') }} <= 0.05 THEN 6 + {{ abs_diff('o.local_amount', 'e.flow_amount') }}
      WHEN e.is_margin = 1 AND o.is_hxswing = 0 AND e.flow_amount > 0 AND o.local_amount >= 0 AND {{ abs_diff('o.local_amount', 'e.flow_amount') }} <= 0.05 THEN 7 + {{ abs_diff('o.local_amount', 'e.flow_amount') }}
      WHEN (e.related_helix_id IS NULL OR e.is_hxswing = 1) AND e.transaction_desc = o.client_reference_number THEN 10
      WHEN e.related_helix_id IS NOT NULL AND o.is_hxswing = 0 AND e.related_helix_id = o.helix_id THEN 20
      WHEN e.is_po = 1 AND PATINDEX(UPPER(e.desc_replaced)+'%', UPPER(o.client_reference_number)) = 1 THEN 30
      WHEN e.related_helix_id IS NULL AND PATINDEX('PMSWING%', UPPER(e.description)) = 1 AND PATINDEX(UPPER(o.client_reference_number)+'%', UPPER(e.description)) = 1 THEN 40
      WHEN o.helix_id IS NOT NULL AND PATINDEX('ERROR_SWING%', UPPER(e.description)) = 1 AND TRY_CAST(SUBSTRING(e.description, 12, len(e.description)) AS INTEGER) = o.helix_id THEN 60
      WHEN o.transaction_type_name = 'DIVIDEND' AND {{ abs_diff('o.local_amount', 'e.flow_amount') }} <= 0.03 THEN 110 + {{ abs_diff('o.local_amount', 'e.flow_amount') }}
      WHEN e.is_po = 1 AND e.flow_amount > 0 AND o.transaction_type_name = 'CASH DEPOSIT' AND {{ abs_diff('o.local_amount', 'e.flow_amount') }} <= {{var('PAIROFF_DIFF_THRESHOLD')}} THEN 120 + {{ abs_diff('o.local_amount', 'e.flow_amount') }}
      WHEN e.is_po = 0 AND e.related_helix_id IS NULL AND e.flow_amount > 0 AND o.transaction_type_name = 'CASH DEPOSIT' AND  {{ abs_diff('o.local_amount', 'e.flow_amount') }} <= 0.01 THEN 140
      WHEN e.is_po = 1 AND PATINDEX(UPPER(o.client_reference_number)+'%', UPPER(e.desc_replaced)) = 1 AND  {{ abs_diff('o.local_amount', 'e.flow_amount') }} <= 0.01 THEN 150
      WHEN e.is_po = 0 AND e.flow_amount < 0 AND o.transaction_type_name = 'BUY' AND  {{ abs_diff('o.local_amount', 'e.flow_amount') }} <= 0.01 THEN 160
      ELSE 9999
    END AS match_rank,
    CASE
      WHEN o.location_name = 'STIF LOCATIONS' AND o.transaction_type_name != 'DIVIDEND' THEN 1
      ELSE 0
    END AS after_sweep
  FROM expected AS e
  LEFT JOIN cash_recon AS o
    ON (
      (e.flow_acct_number = o.short_acct_number
        OR (e.flow_acct_number = 277540 AND o.short_acct_number = 223031)
      ) 
      AND (e.report_date = o.report_date)
    )
),
margin_matches AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY report_date, _flow_id ORDER BY match_rank) AS row_rank
  FROM ranked_matches
  WHERE is_margin = 1 AND margin_total IS NOT NULL AND match_rank < 9999
),
non_margin_matches AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY report_date, _flow_id ORDER BY match_rank) AS row_rank,
    ROW_NUMBER() OVER (PARTITION BY report_date, reference_number ORDER BY match_rank) AS ref_rank
  FROM ranked_matches
  WHERE (is_margin = 0 OR margin_total IS NULL) AND match_rank < 9999
),
flow_matches AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY report_date, _flow_id ORDER BY ref_rank, row_rank) AS rank1
  FROM non_margin_matches
),
ref_matches AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY report_date, reference_number ORDER BY row_rank) AS rank2
  FROM flow_matches
  WHERE rank1 = 1
),
combined_matches AS (
  SELECT
    settle_date AS report_date,
    _flow_id,
    1 AS match_rank,
    reference_number,
    is_settled
  FROM {{ ref('stg_lucid__manual_matches') }}
  UNION ALL
  SELECT
    report_date,
    _flow_id,
    match_rank,
    reference_number,
    NULL AS is_settled
  FROM margin_matches
  WHERE row_rank = 1
  UNION ALL
  SELECT
    report_date,
    _flow_id,
    match_rank,
    reference_number,
    NULL AS is_settled
  FROM ref_matches
  WHERE rank2 = 1
),
sorted_matches AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY report_date, _flow_id ORDER BY match_rank) AS row_rank
  FROM combined_matches
),
final_flows AS (
  SELECT
    o.client_reference_number,
    o.transaction_type_name,
    e.[_flow_id],
    e.[from],
    e.[to],
    e.amount,
    e.is_hxswing,
    e.related_helix_id,
    e.[description],
    e.report_date,
    e.orig_report_date,
    e.desc_replaced,
    e.generated_id,
    e.fund,
    e.series,
    e.[route],
    e.transaction_action_id,
    e.transaction_desc,
    e.flow_account,
    e.flow_acct_number,
    e.flow_security,
    e.flow_status,
    e.flow_amount,
    CASE
      WHEN m.is_settled IS NOT NULL THEN m.is_settled
      ELSE e.flow_is_settled
    END AS flow_is_settled,
    e.flow_after_sweep,
    e.trade_id,
    e.counterparty,
    e.is_margin,
    e.is_po,
    e.margin_total,
    e.used_alloc,
    o.local_amount,
    o.reference_number,
    o.helix_id AS ob_helix_id,
    o.sweep_detected,
    o.cash_account_number,
    CASE
      WHEN m.match_rank IS NOT NULL THEN m.match_rank
      WHEN e.is_margin = 1 AND e.margin_total = 0.0 THEN 0
      ELSE 9999
    END AS match_rank,
    CASE
      WHEN o.location_name = 'STIF LOCATIONS' AND o.transaction_type_name != 'DIVIDEND' THEN 1
      ELSE 0
    END AS after_sweep
  FROM expected AS e
  LEFT JOIN sorted_matches m 
    ON (
      e.report_date = m.report_date
      AND e._flow_id = m._flow_id
      AND row_rank = 1
    )
  LEFT JOIN cash_recon AS o
    ON (
      e.report_date = o.report_date
      AND m.reference_number = o.reference_number
    )
),
final AS (
  SELECT
    *,
    CASE
      WHEN reference_number IS NOT NULL THEN 1
      WHEN is_margin = 1 AND margin_total = 0.0 AND match_rank = 0 THEN 1
      ELSE flow_is_settled
    END AS expected_is_settled,
    CASE
      WHEN is_margin = 1 AND margin_total = 0.0 AND match_rank = 0 THEN flow_after_sweep
      WHEN reference_number IS NOT NULL THEN after_sweep
      ELSE flow_after_sweep
    END AS expected_after_sweep,
    CASE
      WHEN {{ abs_diff('local_amount', 'flow_amount') }} > 0.05 AND is_margin = 1 THEN {{ abs_diff('local_amount', 'margin_total') }} 
      ELSE {{ abs_diff('local_amount', 'flow_amount') }} 
    END AS diff
  FROM final_flows
)

SELECT * FROM final
