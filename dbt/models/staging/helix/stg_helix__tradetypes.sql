{{
    config({
        "as_columnstore": false,
        "materialized": 'table',
        "post-hook": [
            "{{ create_clustered_index(columns = ['tradetype']) }}",
            "{{ create_nonclustered_index(columns = ['description']) }}"
        ]
    })
}}

WITH source AS (SELECT *
                FROM {{ source('helix2', 'helix_raw__stream_TRADETYPES') }}),
     json_data AS (SELECT TRY_CAST(JSON_VALUE(_airbyte_data, '$.BASETYPE') AS INT)           AS basetype,
                          JSON_VALUE(_airbyte_data, '$.CODE')                                AS code,
                          JSON_VALUE(_airbyte_data, '$.DESCRIPTION')                         AS description,
                          JSON_VALUE(_airbyte_data, '$.ENUM')                                AS enum,
                          TRY_CAST(JSON_VALUE(_airbyte_data, '$.INTEREST_DIRECTION') AS INT) AS interest_direction,
                          TRY_CAST(JSON_VALUE(_airbyte_data, '$.MONEY_DIRECTION') AS INT)    AS money_direction,
                          TRY_CAST(JSON_VALUE(_airbyte_data, '$.PAR_DIRECTION') AS INT)      AS par_direction,
                          TRY_CAST(JSON_VALUE(_airbyte_data, '$.TRADETYPE') AS INT)          AS tradetype
                   FROM source),
     renamed AS (SELECT ROW_NUMBER() OVER (PARTITION BY tradetype ORDER BY description DESC) AS row_num, basetype,
                        code,
                        description,
                        enum,
                        interest_direction,
                        money_direction,
                        par_direction,
                        tradetype
                 FROM json_data)

SELECT *
FROM renamed
WHERE row_num = 1;
