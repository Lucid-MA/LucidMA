WITH source AS (
    SELECT
        *
    FROM
        {{ source(
            'helix2',
            'helix_raw__stream_TRADEPIECECALCDATAS'
        ) }}
),
json_data AS (
    SELECT
        TRY_CAST(JSON_VALUE(_airbyte_data, '$.TRADEPIECE') AS BIGINT) AS TRADEPIECE,
        TRY_CAST(JSON_VALUE(_airbyte_data, '$.REPOINTEREST_UNREALIZED') AS MONEY) AS REPOINTEREST_UNREALIZED,
        TRY_CAST(JSON_VALUE(_airbyte_data, '$.REPOINTEREST_NBD') AS MONEY) AS REPOINTEREST_NBD,
        TRY_CAST(JSON_VALUE(_airbyte_data, '$.DATETIMEID') AS DATETIME2) AS DATETIMEID
    FROM source
),
renamed AS (
    SELECT
        tradepiece,
        repointerest_unrealized,
        repointerest_nbd,
        datetimeid
    FROM
        json_data
)
SELECT
    *
FROM
    renamed
