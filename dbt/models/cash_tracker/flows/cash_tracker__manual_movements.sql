{{
    config({
        "as_columnstore": false,
        "materialized": 'table',
        "post-hook": [
            "{{ create_nonclustered_index(columns = ['report_date']) }}",
        ]
    })
}}

WITH 
source AS (
    SELECT
        blotter.*
    FROM
        {{ ref('stg_lucid__cash_blotter') }} AS blotter
),
clean_source AS (
    SELECT
        source.*
    FROM source
    WHERE
        check_pairoff_margin = 0
        AND is_hxswing = 0
),
pairoff_margin AS (
    SELECT
        source.*
    FROM source
      LEFT JOIN {{ ref('cash_tracker__cashpairoffs_summary') }} AS cpo
        ON (
            source.settle_date = cpo.report_date
            AND source.from_fund = cpo.fund
            AND source.cp_name = cpo.counterparty2
        )
    WHERE
        check_pairoff_margin = 1
        AND is_margin = 0
        AND ABS(COALESCE(cpo.amount, 0) + source.amount) > {{var('PAIROFF_DIFF_THRESHOLD')}}
),
combined AS (
    SELECT
        *
    FROM clean_source
    UNION
    SELECT
        *
    FROM pairoff_margin
),
combined2 AS (
    SELECT
        'cash-blotter-outgoing' AS route,
        action_id AS transaction_action_id,
        action_id AS transaction_desc,
        from_acct_name AS flow_account, 
        '{{var('CASH')}}' AS flow_security,
        '{{var('AVAILABLE')}}' AS flow_status,
        -amount AS flow_amount,
        from_fund AS fund,
        '' AS series,
        combined.*
    FROM combined
    WHERE is_outgoing = 1
    UNION
    SELECT
        'cash-blotter-incoming' AS route,
        action_id AS transaction_action_id,
        action_id AS transaction_desc,
        to_acct_name AS flow_account, 
        '{{var('CASH')}}' AS flow_security,
        '{{var('AVAILABLE')}}' AS flow_status,
        amount AS flow_amount,
        to_fund AS fund,
        '' AS series,
        combined.*
    FROM combined
    WHERE is_incoming = 1
),
final AS (
    SELECT
        CASE 
            WHEN action_id LIKE 'ADJ_%' THEN 1
            WHEN action_id LIKE 'MNF_%' THEN 1
            WHEN action_id LIKE 'REALLOC_%' THEN 1
            ELSE NULL
        END AS flow_is_settled,
        CASE 
            WHEN action_id LIKE 'ADJ_%' THEN 0
            WHEN action_id LIKE 'MNF_%' THEN 0
            WHEN action_id LIKE 'REALLOC_%' THEN 0
            ELSE NULL
        END AS flow_after_sweep,
        *
    FROM combined2
)

SELECT
    settle_date AS report_date,
    settle_date AS orig_report_date,
    *
FROM
    final
