WITH
source AS (
  SELECT 
    * 
  FROM {{ source( 'sql2', 'bronze_nexen_cash_balance') }}
),
renamed AS (
  SELECT
    report_date AS balance_date,
    TRIM(a.fund) AS fund,
    NULL AS series,
    TRIM(a.acct_name) AS acct_name,
    CASE
      WHEN sweep_vehicle_number IS NULL THEN '{{var('CASH')}}'
      ELSE sweep_vehicle_number
    END AS [security],
    '{{var('AVAILABLE')}}' AS [status],
    CASE
      WHEN LEFT(beginning_balance_local, 1) = '(' THEN
        CAST('-' + REPLACE(REPLACE(beginning_balance_local, '(', ''), ')', '') AS MONEY)
      ELSE
        CAST(REPLACE(beginning_balance_local, '$', '') AS MONEY)
    END AS beginning_balance,
    CASE
      WHEN LEFT(net_activity_local, 1) = '(' THEN
        CAST('-' + REPLACE(REPLACE(net_activity_local, '(', ''), ')', '') AS MONEY)
      ELSE
        CAST(REPLACE(net_activity_local, '$', '') AS MONEY)
    END AS net_activity,
    CASE
      WHEN LEFT(ending_balance_local, 1) = '(' THEN
        CAST('-' + REPLACE(REPLACE(ending_balance_local, '(', ''), ')', '') AS MONEY)
      ELSE
        CAST(REPLACE(ending_balance_local, '$', '') AS MONEY)
    END AS ending_balance,
    source.*
  FROM source
  JOIN {{ ref('stg_lucid__accounts')}} AS a
  ON (source.account_number = a.acct_number)
),
final AS (
  SELECT
    ending_balance AS amount,
    *
  FROM renamed
)

SELECT * FROM final WHERE cash_account_number LIKE '%8400'
