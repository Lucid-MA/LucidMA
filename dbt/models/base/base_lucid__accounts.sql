WITH
source AS (
  SELECT * FROM {{ ref('stg_lucid__accounts') }}
),
final AS (
  SELECT
    *
  FROM source
)
SELECT * FROM final