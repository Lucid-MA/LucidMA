WITH 
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
        report_date,
        action_id,
        trade_id,
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
                AND startdate = report_date
            )
            OR (
                trade_type = 22
                AND (
                    closedate = report_date
                    OR enddate = report_date
                )
            ) THEN 1
            WHEN (
                trade_type = 22
                AND startdate = report_date
            )
            OR (
                trade_type = 23
                AND (
                    closedate = report_date
                    OR enddate = report_date
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
                    AND startdate = report_date
                ) THEN 'Receive '
                WHEN (
                    trade_type = 22
                    AND startdate = report_date
                ) THEN 'Pay '
                WHEN (
                    trade_type = 23
                    AND (
                        closedate = report_date
                        OR enddate = report_date
                    )
                ) THEN 'Return '
                WHEN (
                    trade_type = 22
                    AND (
                        closedate = report_date
                        OR enddate = report_date
                    )
                ) THEN 'Receive returned '
            END,
            counterparty,
            ' margin'
        ) AS [description]
    FROM
        tradesfree_query
)
SELECT
    *
FROM
    tradesfree_query_on_date
