WITH source AS (
    SELECT
        *
    FROM
        {{ ref('cash_tracker__manual_movements') }}
),
FINAL AS (
    SELECT
        C.report_date,
        C.orig_report_date,
        C.fund,
        ua.series,
        'cash-blotter-series' AS [route],
        C.transaction_action_id,
        C.transaction_desc,
        C.flow_account,
        C.flow_security,
        C.flow_status,
        CASE
            WHEN C.flow_account = 'EXPENSE' THEN 0.0
            ELSE ROUND(amount * ua.used_alloc, 4)
        END AS flow_amount,
        flow_is_settled,
        flow_after_sweep,
        C.trade_id,
        C.action_id,
        C.check_pairoff_margin,
        C.cp_name,
        C.is_hxswing,
        C.is_margin,
        C.ref_id,
        C.related_helix_id,
        ua.used_alloc 
    FROM
        source AS C
        JOIN {{ ref('cash_tracker__used_allocs') }} AS ua
        ON (
            C.related_helix_id IS NOT NULL
            AND C.report_date = ua.report_date
            AND C.fund = ua.fund
            AND C.related_helix_id = ua.trade_id
        )
)
SELECT
    *
FROM
    FINAL
