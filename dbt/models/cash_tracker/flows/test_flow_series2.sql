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
compare AS (
    SELECT
        s.report_date,
        s._flow_id,
        s.fund,
        s.series,
        s.transaction_action_id,
        s.flow_account,
        s.flow_status,
        s.flow_amount,
        SUM(s.flow_amount) OVER (PARTITION BY s.report_date, s._flow_id, s.fund, s.transaction_action_id, s.flow_account, s.flow_status) AS series_total,
        m.flow_amount AS main_amount,
        SUM(s.used_alloc) OVER (PARTITION BY s.report_date, s._flow_id, s.fund, s.transaction_action_id, s.flow_account, s.flow_status) AS total_alloc,
        s.used_alloc
    FROM
        series_flows AS s
    JOIN
        master_flows AS m ON (
            s.report_date = m.report_date
            AND s._flow_id = m._flow_id
            AND s.fund = m.fund
            AND s.transaction_action_id = m.transaction_action_id
            AND s.flow_status = m.flow_status
            AND s.flow_account = m.flow_account
        )
),
final AS (
    SELECT
        report_date,
        _flow_id,
        fund,
        series,
        transaction_action_id,
        flow_account,
        flow_status,
        flow_amount,
        main_amount,
        series_total,
        total_alloc,
        used_alloc
    FROM
        compare
)
SELECT
    *
FROM
    final
