WITH 
source AS (
    SELECT
        blotter.ref_id AS action_id,
        blotter.related_helix_id AS trade_id,
        CASE
          WHEN blotter.from_account IS NULL THEN 0
          ELSE 1
        END is_outgoing,
        CASE
          WHEN blotter.to_account IS NULL THEN 0
          ELSE 1
        END is_incoming,
        from_account.fund AS from_fund,
        from_account.acct_name AS from_acct_name,
        to_account.fund AS to_fund,
        to_account.acct_name AS to_acct_name,
        CASE
          WHEN blotter.from_account IS NOT NULL AND blotter.to_account IS NULL THEN 1
          ELSE 0
        END check_pairoff_margin,
        REPLACE(
          CASE 
            WHEN SUBSTRING(blotter.ref_id, 1, 3) = 'PO ' THEN 
              CASE
                WHEN CHARINDEX(' ', SUBSTRING(blotter.ref_id, 4, LEN(blotter.ref_id))) = 0
                  THEN SUBSTRING(blotter.ref_id, 4, LEN(blotter.ref_id))
                ELSE SUBSTRING(
                  SUBSTRING(blotter.ref_id, 4, LEN(blotter.ref_id)),
                  1,
                  LEN(SUBSTRING(blotter.ref_id, 4, LEN(blotter.ref_id)))
                  - CHARINDEX(' ', REVERSE(SUBSTRING(blotter.ref_id, 4, LEN(blotter.ref_id))))
                )
              END
            ELSE NULL
          END,
          ' ',
          '_'
         ) AS cp_name,
        blotter.*
    FROM
        {{ ref('stg_lucid__cash_blotter') }} AS blotter
        LEFT JOIN {{ ref('stg_lucid__accounts') }} AS from_account
            ON blotter.from_account = from_account.acct_number
        LEFT JOIN {{ ref('stg_lucid__accounts') }} AS to_account
            ON blotter.to_account = to_account.acct_number
    WHERE
        COALESCE(blotter.from_account,0) != COALESCE(blotter.to_account,0)
        AND
        COALESCE(from_account.fund, to_account.fund) IS NOT NULL
),
clean_source AS (
    SELECT
        source.*
    FROM source
    WHERE
        check_pairoff_margin = 0
        AND SUBSTRING(action_id, 1, 7) != 'HXSWING'
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
        AND SUBSTRING(ref_id,0,5) != 'MRGN'
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
final AS (
    SELECT
    'cash-blotter-outgoing' AS route,
    action_id AS transaction_action_id,
    action_id AS transaction_desc,
    from_acct_name AS flow_account, 
    '{{var('CASH')}}' AS flow_security,
    '{{var('AVAILABLE')}}' AS flow_status,
    -amount AS flow_amount,
    from_fund AS fund,
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
    combined.*
    FROM combined
    WHERE is_incoming = 1
)
SELECT
    settle_date AS report_date,
    *
FROM
    final
