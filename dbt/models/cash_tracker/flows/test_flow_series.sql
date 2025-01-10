WITH master_flows AS (
    SELECT
        *
    FROM
        {{ ref('cash_tracker__flows') }}
),
series_flows AS (
    SELECT
        *
    FROM
        {{ ref('cash_tracker__flows_series') }}
),
combined AS (
    SELECT
        *
    FROM
        master_flows
    UNION
    SELECT
        *
    FROM
        series_flows
),
compare AS (
    SELECT
        report_date,
        _flow_id,
        fund,
        transaction_action_id,
        flow_account,
        flow_status,
         SUM(
            CASE
                WHEN series != '' THEN used_alloc
                ELSE 0
            END
        ) AS total_alloc,
        SUM(
            CASE
                WHEN series = '' THEN flow_amount
                ELSE 0
            END
        ) AS main_amount,
        SUM(
            CASE
                WHEN series != '' THEN flow_amount
                ELSE 0
            END
        ) AS series_total
    FROM
        combined
    GROUP BY report_date, _flow_id, fund, transaction_action_id, flow_account, flow_status
),
final AS (
    SELECT
        report_date,
        _flow_id,
        fund,
        transaction_action_id,
        flow_account,
        flow_status,
        total_alloc,
        main_amount,
        --ROUND(series_total, 8) AS series_total
        series_total
    FROM
        compare
)
SELECT
    *
FROM
    final
