WITH source AS (
  SELECT
    *
  FROM
    {{ source(
      'lucid',
      'cash_tracker__balance_history_series'
    ) }}
)

SELECT * FROM source
