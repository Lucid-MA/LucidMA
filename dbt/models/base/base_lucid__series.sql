WITH
source AS (
  SELECT * FROM {{ ref('stg_lucid__series') }}
),
final AS (
  SELECT
    *
  FROM source
)
SELECT * FROM final