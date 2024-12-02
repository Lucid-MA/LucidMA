WITH source AS (
  SELECT
    *
  FROM
    {{ source(
      'lucid',
      'cash_tracker__failing_trades'
    ) }}
)

SELECT * FROM source
