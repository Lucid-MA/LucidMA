WITH source AS (
  SELECT
    *
  FROM
    {{ source(
      'lucid',
      'failing_trade_history'
    ) }}
),
renamed AS (
  SELECT
    TRY_CAST(
      report_date AS DATE
    ) AS report_date,
    TRY_CAST(
      trade_date AS DATE
    ) AS trade_date,
    fund,
    acct_name,
    TRY_CAST(
      amount AS money
    ) AS amount,
    related_id,
    TRIM([description]) AS [description],
    CASE
      WHEN PATINDEX('%[^0-9]%', related_id) = 0 THEN TRY_CAST(related_id AS INT)
      WHEN PATINDEX('%[0-9]%', related_id) > 0 THEN
        TRY_CAST(
          LEFT(
            SUBSTRING(related_id, PATINDEX('%[0-9]%', related_id), LEN(related_id)),
            PATINDEX('%[^0-9]%', SUBSTRING(related_id, PATINDEX('%[0-9]%', related_id), LEN(related_id)) + 'X') - 1
          ) AS INT
        )
      ELSE NULL
    END AS helix_id,
    CASE
      WHEN UPPER(TRIM(description)) LIKE '%MARGIN%' THEN
        CASE
          WHEN UPPER(TRIM(description)) LIKE 'RECEIVE RETURNED%' THEN TRIM(REPLACE(SUBSTRING(UPPER(TRIM(description)),17,LEN(TRIM(description))), ' MARGIN',''))
          ELSE TRIM(REPLACE(SUBSTRING(UPPER(TRIM(description)),PATINDEX('% %',TRIM(description)),LEN(TRIM(description))), ' MARGIN',''))
        END
      ELSE ''
    END AS counterparty
  FROM
    source
)
SELECT
  *
FROM
  renamed
