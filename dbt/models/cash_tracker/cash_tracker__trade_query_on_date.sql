WITH
source AS (
    SELECT
        *
    FROM
        {{ ref('base_cash_tracker__trade_query') }}
),
trade_query AS (
    SELECT
        *
    FROM
        source
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
        report_date,
        company,
        action_id,
        trade_id,
        fund,
        series,
        is_also_master,
        used_alloc,
        trade_type,
        start_date,
        end_date,
        CASE
            WHEN enddate = report_date THEN 1
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
)
SELECT
    *
FROM
    trade_query_on_date
