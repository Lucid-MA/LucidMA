{{
    config({
        "as_columnstore": false,
        "materialized": 'table',
        "post-hook": [
            "{{ create_nonclustered_index(columns = ['report_date']) }}",
            "{{ create_nonclustered_index(columns = ['fund']) }}",
            "{{ create_nonclustered_index(columns = ['counterparty2']) }}",
        ]
    })
}}

WITH
source AS (
    SELECT
      report_date,
      fund,
      counterparty2,
      counterparty,
      sum(amount2) as amount,
      MIN(used_alloc) AS used_alloc
    FROM {{ ref('cash_tracker__cashpairoffs') }}
    GROUP BY report_date, fund, counterparty2, counterparty
),
cashpairoffs_agg AS (
  SELECT 
    report_date,
    report_date AS orig_report_date,
    fund,
    '' AS series,
    'cashpairoffs_agg' AS [route],
    'PO ' + counterparty2 AS transaction_action_id,
    'PO ' + counterparty2 AS transaction_desc,
    'MAIN' AS flow_account, 
    '{{var('CASH')}}' AS flow_security,
    '{{var('AVAILABLE')}}' AS flow_status,
    amount AS flow_amount,
    counterparty,
    counterparty2,
    amount,
    used_alloc
  FROM source
)

SELECT * FROM cashpairoffs_agg