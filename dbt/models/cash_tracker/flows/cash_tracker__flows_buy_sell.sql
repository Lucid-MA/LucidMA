{{
    config({
        "as_columnstore": false,
        "materialized": 'table',
        "post-hook": [
            "{{ create_nonclustered_index(columns = ['report_date']) }}",
            "{{ create_nonclustered_index(columns = ['flow_account']) }}",
            "{{ create_nonclustered_index(columns = ['transaction_action_id']) }}",
        ]
    })
}}

WITH
trades AS (
  SELECT
    *
  FROM {{ ref('base_cash_tracker__trades') }}
),
master_or_only_one_series AS (
  SELECT
    *
  FROM trades
  WHERE (series = 'MASTER' OR is_also_master = 1)
),
buy_sell AS (
  SELECT
    *
  FROM master_or_only_one_series
  WHERE (is_same_date = 0 OR is_roll_of = 0 OR is_rolling_on = 0)
    AND is_trade_rolling = 0
    AND is_buy_sell = 1
),
normal AS (
  SELECT
    *
  FROM master_or_only_one_series
  WHERE (is_same_date = 0 OR is_roll_of = 0 OR is_rolling_on = 0)
    AND is_trade_rolling = 0
    AND is_buy_sell = 0
),
repo_open AS (
  SELECT
    'Option3-repo-open-1' AS route,
    action_id AS transaction_action_id,
    counterparty + ' repo open' AS transaction_desc,
    'MAIN' AS flow_account, 
    security AS flow_security,
    '{{var('REPO_COLLATERAL')}}' AS flow_status,
    -par AS flow_amount,
    *
  FROM normal
  WHERE trade_type = 0 AND is_same_date = 1
  UNION
  SELECT
    'Option3-repo-open-2' AS route,
    action_id AS transaction_action_id,
    counterparty + ' repo open' AS transaction_desc,
    'MAIN' AS flow_account, 
    '{{var('CASH')}}' AS flow_security,
    '{{var('AVAILABLE')}}' AS flow_status,
    [money] AS flow_amount,
    *
  FROM normal
  WHERE trade_type = 0 AND is_same_date = 1
),
repo_term AS (
  SELECT
    'Option3-repo-term-1' AS route,
    action_id AS transaction_action_id,
    counterparty + ' repo term' AS transaction_desc,
    'MAIN' AS flow_account, 
    counterparty AS flow_security,
    '{{var('REPO_COLLATERAL')}}' AS flow_status,
    par AS flow_amount,
    *
  FROM normal
  WHERE trade_type = 0 AND is_same_date = 0
  UNION
  SELECT
    'Option3-repo-term-2' AS route,
    action_id AS transaction_action_id,
    counterparty + ' repo term' AS transaction_desc,
    'MAIN' AS flow_account, 
    '{{var('CASH')}}' AS flow_security,
    '{{var('AVAILABLE')}}' AS flow_status,
    -end_money AS flow_amount,
    *
  FROM normal
  WHERE trade_type = 0 AND is_same_date = 0
),
reverse_repo_open AS (
  SELECT
    'Option3-reverserepo-open-1' AS route,
    action_id AS transaction_action_id,
    counterparty + ' reverse repo open' AS transaction_desc,
    'MAIN' AS flow_account, 
    security AS flow_security,
    '{{var('REPO_COLLATERAL')}}' AS flow_status,
    par AS flow_amount,
    *
  FROM normal
  WHERE trade_type = 1 AND is_same_date = 1
  UNION
  SELECT
    'Option3-reverserepo-open-2' AS route,
    action_id AS transaction_action_id,
    counterparty + ' reverse repo open' AS transaction_desc,
    'MAIN' AS flow_account, 
    '{{var('CASH')}}' AS flow_security,
    '{{var('AVAILABLE')}}' AS flow_status,
    -[money] AS flow_amount,
    *
  FROM normal
  WHERE trade_type = 1 AND is_same_date = 1
),
reverse_repo_term AS (
  SELECT
    'Option3-reverserepo-term-1' AS route,
    action_id AS transaction_action_id,
    counterparty + ' reverse repo term' AS transaction_desc,
    'MAIN' AS flow_account, 
    security AS flow_security,
    '{{var('REPO_COLLATERAL')}}' AS flow_status,
    -par AS flow_amount,
    *
  FROM normal
  WHERE trade_type = 1 AND is_same_date = 0
  UNION
  SELECT
    'Option3-reverserepo-term-2' AS route,
    action_id AS transaction_action_id,
    counterparty + ' reverse repo term' AS transaction_desc,
    'MAIN' AS flow_account, 
    '{{var('CASH')}}' AS flow_security,
    '{{var('AVAILABLE')}}' AS flow_status,
    end_money AS flow_amount,
    *
  FROM normal
  WHERE trade_type = 1 AND is_same_date = 0
),
sell AS (
  SELECT
    'Option3-sell-in' AS route,
    action_id AS transaction_action_id,
    counterparty + ' repo open' AS transaction_desc,
    'MAIN' AS flow_account, 
    '{{var('CASH')}}' AS flow_security,
    '{{var('AVAILABLE')}}' AS flow_status,
    [money] AS flow_amount,
    *
  FROM buy_sell
  WHERE trade_type = 0 AND is_same_date = 1
  UNION
  SELECT
    'Option3-sell-out' AS route,
    action_id AS transaction_action_id,
    counterparty + ' repo open' AS transaction_desc,
    'MAIN' AS flow_account, 
    security AS flow_security,
    '{{var('AVAILABLE')}}' AS flow_status,
    -par AS flow_amount,
    *
  FROM buy_sell
  WHERE trade_type = 0 AND is_same_date = 1
),
buy_back AS (
  SELECT
    'Option3-buyback-out' AS route,
    action_id AS transaction_action_id,
    counterparty + ' repo close' AS transaction_desc,
    'MAIN' AS flow_account, 
    '{{var('CASH')}}' AS flow_security,
    '{{var('AVAILABLE')}}' AS flow_status,
    -end_money AS flow_amount,
    *
  FROM buy_sell
  WHERE trade_type = 0 AND is_same_date = 0
  UNION
  SELECT
    'Option3-buyback-in' AS route,
    action_id AS transaction_action_id,
    counterparty + ' repo close' AS transaction_desc,
    'MAIN' AS flow_account, 
    security AS flow_security,
    '{{var('AVAILABLE')}}' AS flow_status,
    par AS flow_amount,
    *
  FROM buy_sell
  WHERE trade_type = 0 AND is_same_date = 0
),
buy AS (
  SELECT
    'Option3-buy-out' AS route,
    action_id AS transaction_action_id,
    counterparty + 'reverse repo open' AS transaction_desc,
    'MAIN' AS flow_account, 
    '{{var('CASH')}}' AS flow_security,
    '{{var('AVAILABLE')}}' AS flow_status,
    -[money] AS flow_amount,
    *
  FROM buy_sell
  WHERE trade_type = 1 AND is_same_date = 1
  UNION
  SELECT
    'Option3-buy-in' AS route,
    action_id AS transaction_action_id,
    counterparty + 'reverse repo open' AS transaction_desc,
    'MAIN' AS flow_account, 
    security AS flow_security,
    '{{var('AVAILABLE')}}' AS flow_status,
    par AS flow_amount,
    *
  FROM buy_sell
  WHERE trade_type = 1 AND is_same_date = 1
),
sell_back AS (
  SELECT
    'Option3-sellback-in' AS route,
    action_id AS transaction_action_id,
    counterparty + ' reverse repo close' AS transaction_desc,
    'MAIN' AS flow_account, 
    '{{var('CASH')}}' AS flow_security,
    '{{var('AVAILABLE')}}' AS flow_status,
    end_money AS flow_amount,
    *
  FROM buy_sell
  WHERE trade_type = 1 AND is_same_date = 0
  UNION
  SELECT
    'Option3-sellback-out' AS route,
    action_id AS transaction_action_id,
    counterparty + ' reverse repo close' AS transaction_desc,
    'MAIN' AS flow_account, 
    security AS flow_security,
    '{{var('AVAILABLE')}}' AS flow_status,
    -par AS flow_amount,
    *
  FROM buy_sell
  WHERE trade_type = 1 AND is_same_date = 0
),
combined AS (
  SELECT * FROM sell
  UNION
  SELECT * FROM buy_back
  UNION
  SELECT * FROM buy
  UNION
  SELECT * FROM sell_back
  UNION
  SELECT * FROM repo_open
  UNION
  SELECT * FROM repo_term
  UNION
  SELECT * FROM reverse_repo_open
  UNION
  SELECT * FROM reverse_repo_term
),
final AS (
  SELECT
    NULL AS flow_is_settled,
    NULL AS flow_after_sweep,
    *
  FROM combined
)

SELECT * FROM final
