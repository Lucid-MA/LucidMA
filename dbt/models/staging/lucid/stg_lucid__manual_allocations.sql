{{
    config({
        "as_columnstore": false,
        "materialized": 'table',
        "post-hook": [
            "{{ create_nonclustered_index(columns = ['settle_date']) }}",
            "{{ create_nonclustered_index(columns = ['from_account']) }}",
            "{{ create_nonclustered_index(columns = ['to_account']) }}",
        ]
    })
}}

WITH source AS (
    SELECT
        *
    FROM
        {{ source(
            'sql2',
            'bronze_manual_allocation'
        ) }}
),
renamed AS (
    SELECT
        allocation_id,
        TRY_CAST(
            [Settle Date] AS DATE
        ) AS settle_date,
        [Ref ID] AS ref_id,
        TRY_CAST([From Account] AS VARCHAR(100)) AS from_account,
        TRY_CAST([To Account] AS VARCHAR(100)) AS to_account,
        [Series Name] AS series_name,
        TRY_CAST(
            [Amount] AS FLOAT
        ) AS amount,
        [timestamp]
    FROM
        source
)
SELECT
    *
FROM
    renamed
