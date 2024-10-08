WITH source AS (
    SELECT
        *
    FROM
        {{ source(
            'helix',
            'tradepiececalcdatas'
        ) }}
),
renamed AS (
    SELECT
        tradepiece,
        repointerest_unrealized,
        repointerest_nbd
    FROM
        source
)
SELECT
    *
FROM
    renamed
