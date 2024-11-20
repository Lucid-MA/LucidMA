WITH source AS (
    SELECT
        *
    FROM
        {{ source(
            'helix2',
            'helix_raw__stream_TRADECOMMISSIONPIECEINFO'
        ) }}
),
json_data AS (
    SELECT
        TRY_CAST(JSON_VALUE(_airbyte_data, '$.TRADEPIECE') AS BIGINT) AS TRADEPIECE,
        TRY_CAST(JSON_VALUE(_airbyte_data, '$.COMMISSIONVALUE2') AS FLOAT) AS COMMISSIONVALUE2,
        TRY_CAST(JSON_VALUE(_airbyte_data, '$.ROWVERSION') AS TIMESTAMP) AS [ROWVERSION]
    FROM source
),
renamed AS (
    SELECT
        tradepiece,
        commissionvalue2,
        [ROWVERSION]
    FROM
        json_data
)
SELECT
    *
FROM
    renamed
