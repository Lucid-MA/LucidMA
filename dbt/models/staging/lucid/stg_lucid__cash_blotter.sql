WITH source AS (
  SELECT
    *
  FROM
    {{ source(
      'sql2',
      'bronze_cash_blotter'
    ) }}
),
renamed AS (
  SELECT
    cash_blotter_id,
    TRY_CAST(
      [Trade Date] AS DATE
    ) AS trade_date,
    TRY_CAST(
      [Settle Date] AS DATE
    ) AS settle_date,
    [Ref ID] AS ref_id,
    CASE
      WHEN [Related Helix ID] = '#N/A' THEN NULL
      ELSE TRY_CAST(
        [Related Helix ID] AS INT
      )
    END AS related_helix_id,
    TRY_CAST([From Account] AS BIGINT) AS from_account,
    TRY_CAST([To Account] AS BIGINT) AS to_account,
    TRY_CAST(
      [Amount] AS FLOAT
    ) AS amount,
    [timestamp]
  FROM
    source
)
SELECT
  *
FROM
  renamed
