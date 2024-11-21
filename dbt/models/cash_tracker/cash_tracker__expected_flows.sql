 {{
    config({
        "as_columnstore": false,
        "materialized": 'table',
        "post-hook": [
            "{{ create_nonclustered_index(columns = ['report_date']) }}",
            "{{ create_nonclustered_index(columns = ['fund']) }}",
            "{{ create_nonclustered_index(columns = ['trade_id']) }}",
        ]
    })
}}

WITH
accounts AS (
  SELECT
    *
  FROM {{ ref('stg_lucid__accounts') }}
),
cash_recon AS (
  SELECT
    *
  FROM {{ ref('stg_lucid__cash_and_security_transactions') }}
  WHERE 1=1
    --AND TRIM(UPPER(transaction_type_name)) != 'INTERNAL MOVEMENT'
    --AND cusip_cins != '{{var('SWEEP')}}'
),
cash_flows AS (
  SELECT
    f.*,
    a.acct_number AS flow_acct_number
  FROM {{ ref('cash_tracker__flows_plus_allocations') }} AS f
  JOIN accounts AS a ON (f.fund = a.fund AND f.flow_account = a.acct_name)
  WHERE
    flow_security = '{{ var('CASH') }}'
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
  WHERE UPPER(transaction_desc) LIKE '%MARGIN%'
  AND SUBSTRING(transaction_action_id,1,8) != 'REALLOC_'
  GROUP BY report_date, fund, series, counterparty
),
expected AS (
   SELECT 
    CASE
      WHEN flow_amount <= 0 THEN flow_acct_number
      ELSE NULL
    END AS [from],
    CASE
      WHEN flow_amount > 0 THEN flow_acct_number
      ELSE NULL
    END AS [to],
    ABS(flow_amount) AS amount,
    CASE
      WHEN UPPER(SUBSTRING(transaction_desc,1,7)) = 'HXSWING' THEN 1
      ELSE 0 
    END AS is_hxswing,
    trade_id AS related_helix_id,
    transaction_desc AS [description],
    cash_flows.report_date,
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
    CASE
      WHEN PATINDEX('%MARGIN%', UPPER(transaction_desc)) > 0 THEN 1
      ELSE 0
    END AS is_margin,
    CASE
      WHEN UPPER(SUBSTRING(transaction_desc,1,2)) = 'PO' THEN 1
      ELSE 0
    END AS is_po,
    cp_margins.margin_total,
    used_alloc
  FROM cash_flows
  LEFT JOIN cp_margins ON (
    cash_flows.fund = cp_margins.fund
    AND cash_flows.series = cp_margins.series
    AND cash_flows.counterparty = cp_margins.counterparty
    AND cash_flows.report_date = cp_margins.report_date
    AND UPPER(transaction_desc) LIKE '%MARGIN%'
    )
  WHERE SUBSTRING(transaction_action_id,1,8) != 'REALLOC_'
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
      WHEN e.related_helix_id IS NOT NULL AND o.is_hxswing = 0 AND e.related_helix_id = o.helix_id THEN 10
      WHEN (e.related_helix_id IS NULL OR e.is_hxswing = 1) AND e.transaction_desc = o.client_reference_number THEN 20
      WHEN e.is_po = 1 AND PATINDEX(UPPER(e.desc_replaced)+'%', UPPER(o.client_reference_number)) = 1 THEN 30
      WHEN e.related_helix_id IS NULL AND PATINDEX('PMSWING%', UPPER(e.description)) = 1 AND PATINDEX(UPPER(o.client_reference_number)+'%', UPPER(e.description)) = 1 THEN 40
      WHEN e.is_margin = 1 AND e.flow_amount < 0 AND PATINDEX(UPPER('MRGN '+e.counterparty+'%'), UPPER(o.client_reference_number)) = 1 THEN 50
      WHEN e.is_margin = 1 AND e.flow_amount > 0 AND PATINDEX(UPPER('MRGN '+e.counterparty+'%'), UPPER(o.client_reference_number)) = 1 THEN 55
      WHEN o.helix_id IS NOT NULL AND PATINDEX('ERROR_SWING%', UPPER(e.description)) = 1 AND TRY_CAST(SUBSTRING(e.description, 12, len(e.description)) AS INTEGER) = o.helix_id THEN 60
      WHEN e.is_margin = 1 AND e.flow_amount < 0 AND {{ abs_diff('o.local_amount', 'e.flow_amount') }} <= 0.05 THEN 70
      WHEN e.is_margin = 1 AND e.flow_amount > 0 AND o.local_amount >= 0 AND {{ abs_diff('o.local_amount', 'e.flow_amount') }} <= 0.05 THEN 80
      WHEN e.is_margin = 1 AND e.margin_total > 0 AND o.transaction_type_name = 'CASH DEPOSIT' AND  {{ abs_diff('o.local_amount', 'e.margin_total') }} <= 0.05 THEN 90
      WHEN e.is_margin = 1 AND e.margin_total <= 0 AND {{ abs_diff('o.local_amount', 'e.margin_total') }} <= 0.05 THEN 100
      WHEN o.transaction_type_name = 'DIVIDEND' AND {{ abs_diff('o.local_amount', 'e.flow_amount') }} <= 0.03 THEN 110
      WHEN e.is_po = 1 AND e.flow_amount > 0 AND o.transaction_type_name = 'CASH DEPOSIT' AND  {{ abs_diff('o.local_amount', 'e.flow_amount') }} <= {{var('PAIROFF_DIFF_THRESHOLD')}} THEN 120
      WHEN e.is_po = 0 AND e.related_helix_id IS NULL AND e.flow_amount > 0 AND o.transaction_type_name = 'CASH DEPOSIT' AND  {{ abs_diff('o.local_amount', 'e.flow_amount') }} <= 0.01 THEN 130
      WHEN e.is_po = 1 AND PATINDEX(UPPER(o.client_reference_number)+'%', UPPER(e.desc_replaced)) = 1 AND  {{ abs_diff('o.local_amount', 'e.flow_amount') }} <= 0.01 THEN 135
      ELSE 9999
    END AS match_rank,
    CASE
      WHEN o.location_name = 'STIF LOCATIONS' AND o.transaction_type_name != 'DIVIDEND' THEN 1
      ELSE 0
    END AS after_sweep,
    ROW_NUMBER() OVER (PARTITION BY e.generated_id ORDER BY
      CASE
        WHEN e.related_helix_id IS NOT NULL AND o.is_hxswing = 0 AND e.related_helix_id = o.helix_id THEN 10
        WHEN (e.related_helix_id IS NULL OR e.is_hxswing = 1) AND e.transaction_desc = o.client_reference_number THEN 20
        WHEN e.is_po = 1 AND PATINDEX(UPPER(e.desc_replaced)+'%', UPPER(o.client_reference_number)) = 1 THEN 30
        WHEN e.related_helix_id IS NULL AND PATINDEX('PMSWING%', UPPER(e.description)) = 1 AND PATINDEX(UPPER(o.client_reference_number)+'%', UPPER(e.description)) = 1 THEN 40
        WHEN e.is_margin = 1 AND e.flow_amount < 0 AND PATINDEX(UPPER('MRGN '+e.counterparty+'%'), UPPER(o.client_reference_number)) = 1 THEN 50
        WHEN e.is_margin = 1 AND e.flow_amount > 0 AND PATINDEX(UPPER('MRGN '+e.counterparty+'%'), UPPER(o.client_reference_number)) = 1 THEN 55
        WHEN o.helix_id IS NOT NULL AND PATINDEX('ERROR_SWING%', UPPER(e.description)) = 1 AND TRY_CAST(SUBSTRING(e.description, 12, len(e.description)) AS INTEGER) = o.helix_id THEN 60
        WHEN e.is_margin = 1 AND e.flow_amount < 0 AND {{ abs_diff('o.local_amount', 'e.flow_amount') }} <= 0.05 THEN 70
        WHEN e.is_margin = 1 AND e.flow_amount > 0 AND o.local_amount >= 0 AND {{ abs_diff('o.local_amount', 'e.flow_amount') }} <= 0.05 THEN 80
        WHEN e.is_margin = 1 AND e.margin_total > 0 AND o.transaction_type_name = 'CASH DEPOSIT' AND  {{ abs_diff('o.local_amount', 'e.margin_total') }} <= 0.05 THEN 90
        WHEN e.is_margin = 1 AND e.margin_total <= 0 AND {{ abs_diff('o.local_amount', 'e.margin_total') }} <= 0.05 THEN 100
        WHEN o.transaction_type_name = 'DIVIDEND' AND {{ abs_diff('o.local_amount', 'e.flow_amount') }} <= 0.03 THEN 110
        WHEN e.is_po = 1 AND e.flow_amount > 0 AND o.transaction_type_name = 'CASH DEPOSIT' AND  {{ abs_diff('o.local_amount', 'e.flow_amount') }} <= {{var('PAIROFF_DIFF_THRESHOLD')}} THEN 120
        WHEN e.is_po = 0 AND e.related_helix_id IS NULL AND e.flow_amount > 0 AND o.transaction_type_name = 'CASH DEPOSIT' AND  {{ abs_diff('o.local_amount', 'e.flow_amount') }} <= 0.01 THEN 130
        WHEN e.is_po = 1 AND PATINDEX(UPPER(o.client_reference_number)+'%', UPPER(e.desc_replaced)) = 1 AND  {{ abs_diff('o.local_amount', 'e.flow_amount') }} <= 0.01 THEN 135
        ELSE 9999
      END
    ) AS row_rank
  FROM expected AS e
  LEFT JOIN cash_recon AS o
    ON (
      (e.flow_acct_number = o.short_acct_number
        OR (e.flow_acct_number = 277540 AND o.short_acct_number = 223031)
      ) 
      AND (e.report_date = o.report_date)
      AND (
        (e.related_helix_id IS NOT NULL AND o.is_hxswing = 0 AND e.related_helix_id = o.helix_id) OR
        ((e.related_helix_id IS NULL OR e.is_hxswing = 1) AND e.transaction_desc = o.client_reference_number) OR
        (e.is_po = 1 AND PATINDEX(UPPER(e.desc_replaced)+'%', UPPER(o.client_reference_number)) = 1) OR
        (e.related_helix_id IS NULL AND PATINDEX('PMSWING%', UPPER(e.description)) = 1 AND PATINDEX(UPPER(o.client_reference_number)+'%', UPPER(e.description)) = 1) OR
        (e.is_margin = 1 AND e.flow_amount < 0 AND PATINDEX(UPPER('MRGN '+e.counterparty+'%'), UPPER(o.client_reference_number)) = 1) OR
        (e.is_margin = 1 AND e.flow_amount > 0 AND PATINDEX(UPPER('MRGN '+e.counterparty+'%'), UPPER(o.client_reference_number)) = 1) OR
        (o.helix_id IS NOT NULL AND PATINDEX('ERROR_SWING%', UPPER(e.description)) = 1 AND TRY_CAST(SUBSTRING(e.description, 12, len(e.description)) AS INTEGER) = o.helix_id) OR
        (e.is_margin = 1 AND e.flow_amount < 0 AND {{ abs_diff('o.local_amount', 'e.flow_amount') }} <= 0.05) OR
        (e.is_margin = 1 AND e.flow_amount > 0 AND o.local_amount >= 0 AND {{ abs_diff('o.local_amount', 'e.flow_amount') }} <= 0.05) OR
        (e.is_margin = 1 AND e.margin_total > 0 AND o.transaction_type_name = 'CASH DEPOSIT' AND  {{ abs_diff('o.local_amount', 'e.margin_total') }} <= 0.05) OR
        (e.is_margin = 1 AND e.margin_total <= 0 AND {{ abs_diff('o.local_amount', 'e.margin_total') }} <= 0.05) OR
        (o.transaction_type_name = 'DIVIDEND' AND {{ abs_diff('o.local_amount', 'e.flow_amount') }} <= 0.03) OR
        (e.is_po = 1 AND e.flow_amount > 0 AND o.transaction_type_name = 'CASH DEPOSIT' AND  {{ abs_diff('o.local_amount', 'e.flow_amount') }} <= {{var('PAIROFF_DIFF_THRESHOLD')}}) OR
        (e.is_po = 0 AND e.related_helix_id IS NULL AND e.flow_amount > 0 AND o.transaction_type_name = 'CASH DEPOSIT' AND  {{ abs_diff('o.local_amount', 'e.flow_amount') }} <= 0.01) OR
        (e.is_po = 1 AND PATINDEX(UPPER(o.client_reference_number)+'%', UPPER(e.desc_replaced)) = 1)
      )
    )
),
final AS (
  SELECT
    *,
    CASE
      WHEN reference_number IS NOT NULL THEN 1
      ELSE flow_is_settled
    END AS expected_is_settled,
    CASE
      WHEN reference_number IS NOT NULL THEN after_sweep
      ELSE flow_after_sweep
    END AS expected_after_sweep,
    CASE
      WHEN {{ abs_diff('local_amount', 'flow_amount') }} > 0.05 AND is_margin = 1 THEN {{ abs_diff('local_amount', 'margin_total') }} 
      ELSE {{ abs_diff('local_amount', 'flow_amount') }} 
    END AS diff
  FROM ranked_matches
  WHERE row_rank = 1
)

SELECT * FROM final
