WITH
source AS (
  SELECT * FROM {{ ref('series') }}
),
final AS (
  SELECT
    {{ dbt_utils.generate_surrogate_key(['fund','series']) }} as _key,
    fund,
    series,
    nav_ratio,
    sheet_name
  FROM source
)
SELECT * FROM final
