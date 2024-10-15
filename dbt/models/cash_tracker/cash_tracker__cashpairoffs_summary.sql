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
      report_date,
      fund,
      counterparty2,
      counterparty,
      sum(amount2) as amount
    FROM {{ ref('cash_tracker__cashpairoffs') }}
    GROUP BY report_date, fund, counterparty2, counterparty
)

SELECT * FROM source