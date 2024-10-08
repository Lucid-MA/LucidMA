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
trade_query AS (
    SELECT
        *
    FROM
        {{ ref('base_cash_tracker__trade_query') }}
    WHERE
        trade_type IN (
            0,
            1
        )
        AND tradepiece NOT IN (
            37090,
            37089,
            37088,
            37087,
            37086,
            37085,
            37084,
            37083,
            37082,
            37081
        )
),
trade_query_on_date AS (
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
        is_also_master,
        used_alloc,
        trade_type,
        start_date,
        end_date,
        CASE
            WHEN enddate = d THEN 1
            ELSE 0
        END AS set_to_term_on_date,
        security,
        is_buy_sell,
        quantity,
        trade_query.money,
        end_money,
        roll_of,
        counterparty,
        depository
    FROM
        trade_query
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
    trade_query_on_date
