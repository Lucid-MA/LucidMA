{{
    config({
        "as_columnstore": false,
        "materialized": 'table',
        "post-hook": [
            "{{ create_clustered_index(columns = ['tradepiece']) }}",
            "{{ create_nonclustered_index(columns = ['frontofficeid']) }}",
        ]
    })

}}

WITH source AS (
    SELECT
        *
    FROM
        {{ source(
            'helix2',
            'helix_raw__stream_TRADEPIECEXREFS'
        ) }}
),
json_data AS (
    SELECT
        TRY_CAST(JSON_VALUE(_airbyte_data, '$.TRADEPIECE') AS BIGINT) AS TRADEPIECE,
        JSON_VALUE(_airbyte_data, '$.FRONTOFFICEID') AS FRONTOFFICEID,
        TRY_CAST(JSON_VALUE(_airbyte_data, '$.ROWVERSION') AS TIMESTAMP) AS [ROWVERSION]
    FROM source
),
renamed AS (
    SELECT
        tradepiece,
        frontofficeid,
        [ROWVERSION]
    FROM
        json_data
)
SELECT
    *
FROM
    renamed