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
flows AS (
  SELECT
    *
  FROM {{ ref('cash_tracker__flows') }}
),
source AS (
    SELECT
        allocations.ref_id AS action_id,
        CASE
          WHEN allocations.from_account IS NULL THEN 0
          ELSE 1
        END is_outgoing,
        CASE
          WHEN allocations.to_account IS NULL THEN 0
          WHEN from_account.fund = to_account.fund THEN 0
          ELSE 1
        END is_incoming,
        from_account.fund AS from_fund,
        from_account.acct_name AS from_acct_name,
        to_account.fund AS to_fund,
        to_account.acct_name AS to_acct_name,
        allocations.*
    FROM
        {{ ref('stg_lucid__manual_allocations') }} AS allocations
        LEFT JOIN {{ ref('stg_lucid__accounts') }} AS from_account
            ON allocations.from_account = from_account.acct_number
        LEFT JOIN {{ ref('stg_lucid__accounts') }} AS to_account
            ON allocations.to_account = to_account.acct_number
    WHERE
        COALESCE(allocations.from_account,0) != COALESCE(allocations.to_account,0)
        AND
        COALESCE(from_account.fund, to_account.fund) IS NOT NULL
),
combined AS (
    SELECT
        'manual-allocations-outgoing' AS route,
        source.action_id AS transaction_action_id,
        source.action_id AS transaction_desc,
        from_acct_name AS flow_account, 
        '{{var('CASH')}}' AS flow_security,
        '{{var('AVAILABLE')}}' AS flow_status,
        CASE
            WHEN flows.flow_account = 'EXPENSE' THEN 0.0
            WHEN flows.flow_amount = 0 THEN 0.0
            ELSE (-source.amount/flows.flow_amount) * flows.flow_amount 
        END AS flow_amount,
        (-source.amount/NULLIF(flows.flow_amount,0)) AS portion,
        source.amount AS orig_amt,
        flows.flow_amount AS flow_amt, 
        from_fund AS fund,
        series_name AS series,
        flows.trade_id,
        source.*
    FROM source
    JOIN flows ON (
        source.settle_date = flows.report_date
        AND source.from_fund = flows.fund
        AND source.action_id = flows.transaction_action_id
        AND source.from_acct_name = flows.flow_account
        AND flows.flow_security = '{{var('CASH')}}'
        AND flows.flow_status = '{{var('AVAILABLE')}}'
    )
    WHERE is_outgoing = 1
    UNION
     SELECT
        'manual-allocations-outgoing' AS route,
        source.action_id AS transaction_action_id,
        source.action_id AS transaction_desc,
        to_acct_name AS flow_account, 
        '{{var('CASH')}}' AS flow_security,
        '{{var('AVAILABLE')}}' AS flow_status,
        CASE
            WHEN flows.flow_account = 'EXPENSE' THEN 0.0
            WHEN flows.flow_amount = 0 THEN 0.0
            ELSE (source.amount/flows.flow_amount) * flows.flow_amount 
        END AS flow_amount,
        (source.amount/NULLIF(flows.flow_amount,0)) AS portion,
        source.amount AS orig_amt,
        flows.flow_amount AS flow_amt,
        to_fund AS fund,
        series_name AS series,
        flows.trade_id,
        source.*
    FROM source
    JOIN flows ON (
        source.settle_date = flows.report_date
        AND source.to_fund = flows.fund
        AND source.action_id = flows.transaction_action_id
        AND source.to_acct_name = flows.flow_account
        AND flows.flow_security = '{{var('CASH')}}'
        AND flows.flow_status = '{{var('AVAILABLE')}}'
    )
    WHERE is_incoming = 1
),
final AS (
    SELECT
        *
    FROM combined
)

SELECT
    settle_date AS report_date,
    NULL as flow_is_settled,
    NULL as flow_after_sweep,
    *
FROM
    final
