WITH source AS (
    SELECT
        *
    FROM
        {{ source(
            'helix',
            'tradepiecexrefs'
        ) }}
),
renamed AS (
    SELECT
        tradepiece,
        frontofficeid
    FROM
        source
)
SELECT
    *
FROM
    renamed
