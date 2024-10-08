WITH source AS (
    SELECT
        *
    FROM
        {{ source(
            'helix',
            'tradecommissionpieceinfo'
        ) }}
),
renamed AS (
    SELECT
        tradepiece,
        commissionvalue2
    FROM
        source
)
SELECT
    *
FROM
    renamed
