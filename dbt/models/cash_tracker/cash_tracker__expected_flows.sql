{{
    config({
        "as_columnstore": false,
        "materialized": 'table',
        "post-hook": [
            "{{ create_nonclustered_index(columns = ['report_date']) }}",
            "{{ create_nonclustered_index(columns = ['fund']) }}",
            "{{ create_nonclustered_index(columns = ['series']) }}",
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
    counterparty,
    SUM(flow_amount) AS margin_total
  FROM cash_flows
  WHERE UPPER(transaction_desc) LIKE '%MARGIN%'
  AND SUBSTRING(transaction_action_id,1,8) != 'REALLOC_'
  GROUP BY counterparty, report_date
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
    fund,
    series,
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
    cash_flows.counterparty = cp_margins.counterparty
    AND cash_flows.report_date = cp_margins.report_date
    AND UPPER(transaction_desc) LIKE '%MARGIN%'
    )
  WHERE SUBSTRING(transaction_action_id,1,8) != 'REALLOC_'
),
ranked_matches AS (
  SELECT
    e.*,
    o.local_amount,
    o.reference_number,
    o.helix_id AS ob_helix_id,
    o.sweep_detected,
    CASE
      WHEN e.related_helix_id IS NOT NULL AND o.is_hxswing = 0 AND e.related_helix_id = o.helix_id THEN 2
      WHEN (e.related_helix_id IS NULL OR e.is_hxswing = 1) AND e.transaction_desc = o.client_reference_number THEN 4
      WHEN e.is_po = 1 AND PATINDEX(UPPER(e.desc_replaced)+'%', UPPER(o.client_reference_number)) = 1 THEN 6
      WHEN e.is_margin = 1 AND e.flow_amount < 0 AND PATINDEX(UPPER('MRGN '+e.counterparty+'%'), UPPER(o.client_reference_number)) = 1 THEN 8
      WHEN e.is_margin = 1 AND e.flow_amount < 0 AND {{ abs_diff('o.local_amount', 'e.flow_amount') }} <= 0.05 THEN 10
      WHEN e.is_margin = 1 AND e.flow_amount > 0 AND o.local_amount >= 0 AND {{ abs_diff('o.local_amount', 'e.flow_amount') }} <= 0.05 THEN 11
      WHEN e.is_margin = 1 AND e.margin_total > 0 AND o.transaction_type_name = 'CASH DEPOSIT' AND  {{ abs_diff('o.local_amount', 'e.margin_total') }} <= 0.05 THEN 12
      WHEN e.is_margin = 1 AND e.margin_total <= 0 AND {{ abs_diff('o.local_amount', 'e.margin_total') }} <= 0.05 THEN 14
      WHEN o.transaction_type_name = 'DIVIDEND' AND {{ abs_diff('o.local_amount', 'e.flow_amount') }} <= 0.03 THEN 16
      WHEN e.is_po = 1 AND e.flow_amount > 0 AND o.transaction_type_name = 'CASH DEPOSIT' AND  {{ abs_diff('o.local_amount', 'e.flow_amount') }} <= {{var('PAIROFF_DIFF_THRESHOLD')}} THEN 18
      WHEN e.flow_amount > 0 AND o.transaction_type_name = 'CASH DEPOSIT' AND  {{ abs_diff('o.local_amount', 'e.flow_amount') }} <= 0.01 THEN 20
      ELSE 9999
    END AS match_rank,
    CASE
      WHEN o.location_name = 'STIF LOCATIONS' AND o.transaction_type_name != 'DIVIDEND' THEN 1
      ELSE 0
    END AS after_sweep,
    ROW_NUMBER() OVER (PARTITION BY e.generated_id ORDER BY
      CASE
        WHEN e.related_helix_id IS NOT NULL AND o.is_hxswing = 0 AND e.related_helix_id = o.helix_id THEN 2
        WHEN (e.related_helix_id IS NULL OR e.is_hxswing = 1) AND e.transaction_desc = o.client_reference_number THEN 4
        WHEN e.is_po = 1 AND PATINDEX(UPPER(e.desc_replaced)+'%', UPPER(o.client_reference_number)) = 1 THEN 6
        WHEN e.is_margin = 1 AND e.flow_amount < 0 AND PATINDEX(UPPER('MRGN '+e.counterparty+'%'), UPPER(o.client_reference_number)) = 1 THEN 8
        WHEN e.is_margin = 1 AND e.flow_amount < 0 AND {{ abs_diff('o.local_amount', 'e.flow_amount') }} <= 0.05 THEN 10
        WHEN e.is_margin = 1 AND e.flow_amount > 0 AND o.local_amount >= 0 AND {{ abs_diff('o.local_amount', 'e.flow_amount') }} <= 0.05 THEN 11
        WHEN e.is_margin = 1 AND e.margin_total > 0 AND o.transaction_type_name = 'CASH DEPOSIT' AND  {{ abs_diff('o.local_amount', 'e.margin_total') }} <= 0.05 THEN 12
        WHEN e.is_margin = 1 AND e.margin_total <= 0 AND {{ abs_diff('o.local_amount', 'e.margin_total') }} <= 0.05 THEN 14
        WHEN o.transaction_type_name = 'DIVIDEND' AND {{ abs_diff('o.local_amount', 'e.flow_amount') }} <= 0.03 THEN 16
        WHEN e.is_po = 1 AND e.flow_amount > 0 AND o.transaction_type_name = 'CASH DEPOSIT' AND  {{ abs_diff('o.local_amount', 'e.flow_amount') }} <= {{var('PAIROFF_DIFF_THRESHOLD')}} THEN 18
        WHEN e.flow_amount > 0 AND o.transaction_type_name = 'CASH DEPOSIT' AND  {{ abs_diff('o.local_amount', 'e.flow_amount') }} <= 0.01 THEN 20
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
        (e.is_margin = 1 AND e.flow_amount < 0 AND PATINDEX(UPPER('MRGN '+e.counterparty+'%'), UPPER(o.client_reference_number)) = 1) OR
        (e.is_margin = 1 AND e.flow_amount < 0 AND {{ abs_diff('o.local_amount', 'e.flow_amount') }} <= 0.05) OR
        (e.is_margin = 1 AND e.flow_amount > 0 AND o.local_amount >= 0 AND {{ abs_diff('o.local_amount', 'e.flow_amount') }} <= 0.05) OR
        (e.is_margin = 1 AND e.margin_total > 0 AND o.transaction_type_name = 'CASH DEPOSIT' AND  {{ abs_diff('o.local_amount', 'e.margin_total') }} <= 0.05) OR
        (e.is_margin = 1 AND e.margin_total <= 0 AND {{ abs_diff('o.local_amount', 'e.margin_total') }} <= 0.05) OR
        (o.transaction_type_name = 'DIVIDEND' AND {{ abs_diff('o.local_amount', 'e.flow_amount') }} <= 0.03) OR
        (e.is_po = 1 AND e.flow_amount > 0 AND o.transaction_type_name = 'CASH DEPOSIT' AND  {{ abs_diff('o.local_amount', 'e.flow_amount') }} <= {{var('PAIROFF_DIFF_THRESHOLD')}}) OR
        (o.is_hxswing = 0 AND e.flow_amount > 0 AND o.transaction_type_name = 'CASH DEPOSIT' AND  {{ abs_diff('o.local_amount', 'e.flow_amount') }} <= 0.01)
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
