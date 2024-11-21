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
        [From Account] AS from_account,
        [To Account] AS to_account,
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
