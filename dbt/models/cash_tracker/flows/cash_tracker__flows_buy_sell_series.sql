WITH trades AS (
  SELECT
    *
  FROM
    {{ ref('base_cash_tracker__trades') }}
),
buy_sell AS (
  SELECT
    *
  FROM
    {{ ref('cash_tracker__flows_buy_sell') }}
),
new_rolling_on_by_date AS (
  SELECT
    report_date,
    trade_id
  FROM
    trades
  WHERE
    (
      series = 'MASTER'
      OR is_also_master = 1
    ) --AND report_date = start_date
    AND is_same_date = 1
    AND is_roll_of = 1
    AND is_rolling_on = 1
),
series_trades AS (
  SELECT
    *,
    CASE
      WHEN trade_id IN (
        SELECT
          trade_id
        FROM
          new_rolling_on_by_date AS r
        WHERE
          r.report_date = trades.report_date
          AND r.trade_id = trades.trade_id
      ) THEN 1
      ELSE 0
    END AS is_new_trade_rolling
  FROM
    trades
),
not_roll AS (
  SELECT
    'Part3' AS route,
    bs.transaction_action_id,
    bs.transaction_desc,
    bs.flow_account,
    bs.flow_security,
    bs.flow_status,
    CASE
      WHEN bs.flow_account = 'EXPENSE' THEN 0.0
      ELSE (
        bs.flow_amount * st.used_alloc
      )
    END AS flow_amount,
    st.*
  FROM
    series_trades st
    LEFT JOIN buy_sell bs
    ON (
      st.action_id = bs.transaction_action_id
      AND st.report_date = bs.report_date
    )
  WHERE
    st.series != 'MASTER'
    AND st.is_new_trade_rolling = 0
    AND st.is_trade_rolling = 0
),
FINAL AS (
  SELECT
    NULL AS flow_is_settled,
    NULL AS flow_after_sweep,
    [route],
    transaction_action_id,
    transaction_desc,
    flow_account,
    flow_security,
    flow_status,
    flow_amount,
    report_date,
    orig_report_date,
    company,
    action_id,
    trade_id,
    fund,
    series,
    is_also_master,
    used_alloc,
    trade_type,
    [start_date],
    end_date,
    set_to_term_on_date,
    [security],
    is_buy_sell,
    quantity,
    [money],
    end_money,
    roll_of,
    counterparty,
    depository,
    par,
    is_same_date,
    is_roll_of,
    is_rolling_on,
    is_trade_rolling,
    modifier
  FROM
    not_roll
)
SELECT
  *
FROM
  FINAL
