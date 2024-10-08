WITH source AS (
    SELECT
        *
    FROM
        {{ source(
            'lucid',
            'manual_allocations'
        ) }}
),
renamed AS (
    SELECT
        TRY_CAST(
            settle_date AS DATE
        ) AS settle_date,
        ref_id,
        from_account,
        to_account,
        series_name,
        TRY_CAST(
            amount AS money
        ) AS amount
    FROM
        source
)
SELECT
    *
FROM
    renamed
