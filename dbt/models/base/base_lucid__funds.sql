WITH
source AS (
  SELECT * FROM {{ ref('stg_lucid__funds') }}
),
final AS (
  SELECT
    *
  FROM source
)
SELECT * FROM final