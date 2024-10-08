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
        AND d < '2024-09-30'
),
tradesfree_query AS (
    SELECT
        *
    FROM
        {{ ref('base_cash_tracker__trade_query') }}
    WHERE
        trade_type IN (
            22,
            23
        )
),
tradesfree_query_on_date AS (
    SELECT
        dates.d AS report_date,
        CONCAT(
            action_id_prefix,
            ' ',
            (
                CASE
                    WHEN startdate = d THEN 'TRANSMITTED'
                    ELSE 'CLOSED'
                END
            )
        ) AS action_id,
        action_id_prefix AS trade_id,
        fund,
        series,
        used_alloc,
        is_also_master,
        start_date,
        closedate AS close_date,
        enddate AS end_date,
        quantity * CASE
            WHEN (
                trade_type = 23
                AND startdate = d
            )
            OR (
                trade_type = 22
                AND (
                    closedate = d
                    OR enddate = d
                )
            ) THEN 1
            WHEN (
                trade_type = 22
                AND startdate = d
            )
            OR (
                trade_type = 23
                AND (
                    closedate = d
                    OR enddate = d
                )
            ) THEN -1
            ELSE 0
        END AS amount,
        trade_type,
        security,
        counterparty,
        CONCAT(
            CASE
                WHEN (
                    trade_type = 23
                    AND startdate = d
                ) THEN 'Receive '
                WHEN (
                    trade_type = 22
                    AND startdate = d
                ) THEN 'Pay '
                WHEN (
                    trade_type = 23
                    AND (
                        closedate = d
                        OR enddate = d
                    )
                ) THEN 'Return '
                WHEN (
                    trade_type = 22
                    AND (
                        closedate = d
                        OR enddate = d
                    )
                ) THEN 'Receive returned '
            END,
            counterparty,
            ' margin'
        ) AS [description]
    FROM
        tradesfree_query
        JOIN dates
        ON 1 = 1
    WHERE
        (
            startdate = d
            OR CASE
                WHEN closedate IS NULL THEN enddate
                ELSE closedate
            END = dates.d
        )
)
SELECT
    *
FROM
    tradesfree_query_on_date
