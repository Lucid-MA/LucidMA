WITH
source AS (
  SELECT 
    * 
  FROM {{ source( 'sql2', 'bronze_cash_balance') }}
),
renamed AS (
  SELECT
    Balance_ID AS balance_id,
    Balance_date AS balance_date,
    Fund AS fund,
    Series AS series,
    Account AS account,
    Cash_Balance AS cash_balance,
    Sweep_Balance AS sweep_balance,
    Projected_Total_Balance AS projected_total_balance,
    Source AS source
  FROM source
),
final AS (
  SELECT
    *
  FROM renamed
)

SELECT * FROM final
