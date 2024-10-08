WITH
source AS (
  SELECT * FROM {{ ref('accounts') }}
),
final AS (
  SELECT
    {{ dbt_utils.generate_surrogate_key(['fund','acct_name']) }} as _key,
    fund,
    acct_name,
    acct_number
  FROM source
)
SELECT * FROM final
