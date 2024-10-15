WITH source AS (
    SELECT
        *
    FROM
        {{ source(
            'helix',
            'tradepieces'
        ) }}
),
renamed AS (
    SELECT
        tradepiece,
        statusmain,
        company,
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
        ledgername
    FROM
        source
)
SELECT
    *
FROM
    renamed
