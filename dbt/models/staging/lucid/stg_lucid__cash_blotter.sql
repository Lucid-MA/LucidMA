WITH source AS (
  SELECT
    *
  FROM
    {{ source(
      'lucid',
      'cash_blotter'
    ) }}
),
renamed AS (
  SELECT
    TRY_CAST(
      trade_date AS DATE
    ) AS trade_date,
    TRY_CAST(
      settle_date AS DATE
    ) AS settle_date,
    ref_id,
    CASE
      WHEN related_helix_id = '#N/A' THEN NULL
      ELSE TRY_CAST(
        related_helix_id AS INT
      )
    END AS related_helix_id,
    from_account,
    to_account,
    TRY_CAST(
      amount AS money
    ) AS amount
  FROM
    source
)
SELECT
  *
FROM
  renamed
