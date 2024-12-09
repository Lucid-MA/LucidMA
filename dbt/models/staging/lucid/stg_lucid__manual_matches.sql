WITH source AS (
  SELECT
    *
  FROM
    {{ source(
      'sql2',
      'cash_tracker_manual_matches'
    ) }}
)

SELECT * FROM source
