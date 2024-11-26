{{
    config({
        "as_columnstore": false,
        "materialized": 'table',
        "post-hook": [
            "{{ create_clustered_index(columns = ['tradepiece']) }}",
            "{{ create_nonclustered_index(columns = ['company']) }}",
            "{{ create_nonclustered_index(columns = ['statusmain']) }}",
        ]
    })

}}

WITH source AS (
    SELECT
        *
    FROM
        {{ source(
            'helix2',
            'helix_raw__stream_TRADEPIECES'
        ) }}
),
json_data AS (
    SELECT
        TRY_CAST(JSON_VALUE(_airbyte_data, '$.TRADEPIECE') AS BIGINT) AS TRADEPIECE,
        --TRY_CAST(JSON_VALUE(_airbyte_data, '$.TRADESHELL') AS BIGINT) AS TRADESHELL,
        TRY_CAST(JSON_VALUE(_airbyte_data, '$.STATUSMAIN') AS INT) AS STATUSMAIN,
        --TRY_CAST(JSON_VALUE(_airbyte_data, '$.STATUSDETAIL') AS INT) AS STATUSDETAIL,
        TRY_CAST(JSON_VALUE(_airbyte_data, '$.COMPANY') AS INT) AS COMPANY,
        --TRY_CAST(JSON_VALUE(_airbyte_data, '$.USERID') AS INT) AS USERID,
        TRY_CAST(JSON_VALUE(_airbyte_data, '$.DATETIMEID') AS DATETIME2) AS DATETIMEID,
        --TRY_CAST(JSON_VALUE(_airbyte_data, '$.REVISIONID') AS INT) AS REVISIONID,
        TRY_CAST(JSON_VALUE(_airbyte_data, '$.ENTERDATETIMEID') AS DATETIME2) AS ENTERDATETIMEID,
        --TRY_CAST(JSON_VALUE(_airbyte_data, '$.BOOKDATE') AS DATETIME2) AS BOOKDATE,
        --TRY_CAST(JSON_VALUE(_airbyte_data, '$.ALLOCATEDATE') AS DATETIME2) AS ALLOCATEDATE,
        TRY_CAST(JSON_VALUE(_airbyte_data, '$.TRADEDATE') AS DATETIME2) AS TRADEDATE,
        TRY_CAST(JSON_VALUE(_airbyte_data, '$.STARTDATE') AS DATETIME2) AS STARTDATE,
        TRY_CAST(JSON_VALUE(_airbyte_data, '$.CLOSEDATE') AS DATETIME2) AS CLOSEDATE,
        TRY_CAST(JSON_VALUE(_airbyte_data, '$.ENDDATE') AS DATETIME2) AS ENDDATE,
        TRY_CAST(JSON_VALUE(_airbyte_data, '$.ISGSCC') AS BIT) AS ISGSCC,
        --TRY_CAST(JSON_VALUE(_airbyte_data, '$.YIELD') AS FLOAT) AS YIELD,
        --TRY_CAST(JSON_VALUE(_airbyte_data, '$.ISSUE') AS INT) AS ISSUE,
        --JSON_VALUE(_airbyte_data, '$.AUCTION') AS AUCTION,
        --TRY_CAST(JSON_VALUE(_airbyte_data, '$.CURRENCY_PAR') AS INT) AS CURRENCY_PAR,
        --TRY_CAST(JSON_VALUE(_airbyte_data, '$.CURRENCY_MONEY') AS INT) AS CURRENCY_MONEY,
        --TRY_CAST(JSON_VALUE(_airbyte_data, '$.FX_FACTOR') AS FLOAT) AS FX_FACTOR,
        --TRY_CAST(JSON_VALUE(_airbyte_data, '$.FX_PAR') AS FLOAT) AS FX_PAR,
        --TRY_CAST(JSON_VALUE(_airbyte_data, '$.FX_MONEY') AS FLOAT) AS FX_MONEY,
        TRY_CAST(JSON_VALUE(_airbyte_data, '$.PAR') AS MONEY) AS PAR,
        --TRY_CAST(JSON_VALUE(_airbyte_data, '$.PRICE') AS FLOAT) AS PRICE,
        TRY_CAST(JSON_VALUE(_airbyte_data, '$.MONEY') AS MONEY) AS [MONEY],
        TRY_CAST(JSON_VALUE(_airbyte_data, '$.TRADETYPE') AS INT) AS TRADETYPE,
        JSON_VALUE(_airbyte_data, '$.DEPOSITORY') AS DEPOSITORY,
        --TRY_CAST(JSON_VALUE(_airbyte_data, '$.INTEREST') AS FLOAT) AS INTEREST,
        --TRY_CAST(JSON_VALUE(_airbyte_data, '$.ISVISIBLE') AS BIT) AS ISVISIBLE,
        --JSON_VALUE(_airbyte_data, '$.COMMENTS') AS COMMENTS,
        JSON_VALUE(_airbyte_data, '$.CUSIP') AS CUSIP,
        --JSON_VALUE(_airbyte_data, '$.ISIN') AS ISIN,
        --TRY_CAST(JSON_VALUE(_airbyte_data, '$.MATURITY') AS DATETIME2) AS MATURITY,
        JSON_VALUE(_airbyte_data, '$.ACCT_NUMBER') AS ACCT_NUMBER,
        JSON_VALUE(_airbyte_data, '$.LEDGERNAME') AS LEDGERNAME
    --FROM Prod1.airbyte_internal.helix_raw__stream_TRADEPIECES
    FROM source
),
renamed AS (
    SELECT
        ROW_NUMBER() OVER (PARTITION by tradepiece ORDER BY datetimeid DESC) AS row_num,
        tradepiece,
        statusmain,
        company,
        TRY_CAST(tradedate as DATE) as tradedate,
        TRY_CAST(startdate as DATE) as startdate,
        TRY_CAST(closedate as DATE) as closedate,
        TRY_CAST(enddate as DATE) as enddate,
        isgscc,
        par,
        [MONEY],
        tradetype,
        depository,
        cusip,
        acct_number,
        ledgername,
        datetimeid
    FROM
        json_data
)
SELECT
    *
FROM
    renamed
WHERE row_num = 1