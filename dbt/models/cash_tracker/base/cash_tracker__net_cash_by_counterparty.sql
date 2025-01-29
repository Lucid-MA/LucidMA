{{
    config({
        "as_columnstore": false,
        "materialized": 'table',
        "post-hook": [
            "{{ create_nonclustered_index(columns = ['report_date']) }}",
        ]
    })
}}

WITH dates AS (
    SELECT
        d
    FROM
        {{ ref("util_date__window") }}
    WHERE
        d > '2024-09-01'
        AND d < CAST(GETDATE() AS DATE)
),
tradepieces AS (
    SELECT
        *
    FROM
        {{ ref('stg_helix__tradepieces') }}
),
table1 AS (
    SELECT
        d AS report_date,
        CASE
            WHEN company = 44 THEN 'USG'
            WHEN company = 45 THEN 'PRIME'
            WHEN company = 46 THEN 'MNT'
            ELSE 'Other'
        END AS fund,
        TRIM(UPPER(acct_number)) AS acct_number,
        LTRIM(RTRIM(ledgername)) AS ledgername,
        ROUND(
            SUM(
                CASE
                    WHEN tradetype = 22 THEN -1
                    ELSE 1
                END 
                * 
                CASE
                    WHEN (closedate = d OR enddate = d) THEN 0
                    ELSE 1
                END 
                *
                par
            ),
            2
        ) AS net_cash,
        CASE
            WHEN NOT company = 45 THEN 1
            ELSE 0
        END AS is_also_master
    FROM
        tradepieces
        JOIN dates
            ON 1 = 1
        WHERE
            (startdate <= d
                AND (
                closedate >= d
                OR (
                    (
                        enddate IS NULL
                        OR enddate >= d
                    )
                    AND closedate IS NULL
                )
            )
            )
            AND company IN (
                44,
                45
            )
            AND tradetype IN (
                22,
                23
            )
            AND cusip = '{{var('CASH')}}'
            AND statusmain NOT IN (6)
        GROUP BY
            d,
            company,
            ledgername,
            TRIM(UPPER(acct_number))
),
table2 AS (
    SELECT
        d AS report_date,
        CASE
            WHEN company = 44 THEN 'USG'
            WHEN company = 45 THEN 'PRIME'
            WHEN company = 46 THEN 'MNT'
            ELSE 'Other'
        END AS fund,
        TRIM(UPPER(acct_number)) AS acct_number,
        LTRIM(RTRIM(ledgername)) AS ledgername,
        ROUND(
            SUM(
                CASE
                    WHEN tradetype = 22 THEN -1
                    ELSE 1
                END 
                *
                CASE
                    WHEN startdate = d THEN 1
                    ELSE -1
                END
                *
                par
            ),
            2
        ) AS activity,
        CASE
            WHEN NOT company = 45 THEN 1
            ELSE 0
        END AS is_also_master
    FROM
        tradepieces
        JOIN dates
            ON 1 = 1
        WHERE
            (startdate = d
            OR
            closedate = d
            OR
            (enddate = d and closedate is null)
            )
            AND company IN (
                44,
                45
            )
            AND tradetype IN (
                22,
                23
            )
            AND cusip = '{{var('CASH')}}'
            AND statusmain NOT IN (6)
        GROUP BY
            d,
            company,
            ledgername,
            TRIM(UPPER(acct_number))
),
FINAL AS (
    SELECT
        table1.report_date,
        table1.fund,
        table1.acct_number,
        table1.acct_number AS counterparty,
        table1.ledgername,
        UPPER(table1.ledgername) series,
        table1.net_cash,
        CASE
            WHEN table2.activity IS NULL THEN 0
            ELSE table2.activity
        END AS activity,
        table1.is_also_master
    FROM
        table1
        FULL OUTER JOIN
        table2
            ON  table1.report_date = table2.report_date 
            AND table1.fund = table2.fund
            AND table1.acct_number = table2.acct_number
            AND table1.ledgername = table2.ledgername
)
SELECT
    *
FROM
    FINAL
