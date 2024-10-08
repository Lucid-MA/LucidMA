WITH
source AS (
  SELECT * FROM {{ ref('funds') }}
),
final AS (
  SELECT
    {{ dbt_utils.generate_surrogate_key(['fund']) }} as _key,
    fund,
    sweep_vehicle
  FROM source
)
SELECT * FROM final
