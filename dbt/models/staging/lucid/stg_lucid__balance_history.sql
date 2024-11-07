WITH
source AS (
  SELECT 
    * 
  FROM {{ source( 'sql2', 'bronze_nexen_cash_balance') }}
),
final AS (
  SELECT
    report_date AS balance_date,
    TRIM(a.fund) AS fund,
    '' AS series,
    TRIM(a.acct_name) AS acct_name,
    CASE
      WHEN sweep_vehicle_number IS NULL THEN '{{var('CASH')}}'
      ELSE sweep_vehicle_number
    END AS [security],
    '{{var('AVAILABLE')}}' AS [status],
    TRY_CAST(
      ending_balance_local AS money
    ) AS amount,
    source.*
  FROM source
  JOIN {{ ref('stg_lucid__accounts')}} AS a
  ON (source.account_number = a.acct_number)
)
SELECT * FROM final WHERE cash_account_number LIKE '%8400'