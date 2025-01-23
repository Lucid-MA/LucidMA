{{
    config({
        "as_columnstore": false,
        "materialized": 'table',
        "post-hook": [
            "{{ create_nonclustered_index(columns = ['settle_date']) }}",
            "{{ create_nonclustered_index(columns = ['from_fund']) }}",
            "{{ create_nonclustered_index(columns = ['to_fund']) }}",
            "{{ create_nonclustered_index(columns = ['cp_name']) }}",
            "{{ create_nonclustered_index(columns = ['is_hxswing']) }}",
            "{{ create_nonclustered_index(columns = ['check_pairoff_margin']) }}",
        ]
    })
}}

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
),
final AS (
    SELECT
        blotter.ref_id AS action_id,
        blotter.related_helix_id AS trade_id,
        CASE
          WHEN blotter.from_account IS NULL THEN 0
          ELSE 1
        END is_outgoing,
        CASE
          WHEN blotter.to_account IS NULL THEN 0
          ELSE 1
        END is_incoming,
        from_account.fund AS from_fund,
        from_account.acct_name AS from_acct_name,
        to_account.fund AS to_fund,
        to_account.acct_name AS to_acct_name,
        CASE
          WHEN blotter.from_account IS NOT NULL AND blotter.to_account IS NULL THEN 1
          ELSE 0
        END check_pairoff_margin,
        TRY_CAST(SUBSTRING(REPLACE(
          CASE 
            WHEN SUBSTRING(blotter.ref_id, 1, 3) = 'PO ' THEN 
              CASE
                WHEN CHARINDEX(' ', SUBSTRING(blotter.ref_id, 4, LEN(blotter.ref_id))) = 0
                  THEN SUBSTRING(blotter.ref_id, 4, LEN(blotter.ref_id))
                ELSE SUBSTRING(
                  SUBSTRING(blotter.ref_id, 4, LEN(blotter.ref_id)),
                  1,
                  LEN(SUBSTRING(blotter.ref_id, 4, LEN(blotter.ref_id)))
                  - CHARINDEX(' ', REVERSE(SUBSTRING(blotter.ref_id, 4, LEN(blotter.ref_id))))
                )
              END
            ELSE NULL
          END,
          ' ',
          '_'
         ), 1, 1000) AS VARCHAR(1000)) AS cp_name,
        CASE
          WHEN UPPER(SUBSTRING(blotter.ref_id,1,7)) = 'HXSWING' THEN 1
          ELSE 0 
        END AS is_hxswing,
        CASE
          WHEN UPPER(SUBSTRING(ref_id,0,5)) = 'MRGN' THEN 1
          ELSE 0
        END AS is_margin,
        CASE 
          WHEN UPPER(SUBSTRING(ref_id,1,2)) = 'PO' THEN 1 
          ELSE 0 
        END AS is_po,
        blotter.*
    FROM
        renamed AS blotter
        LEFT JOIN {{ ref('stg_lucid__accounts') }} AS from_account
            ON blotter.from_account = from_account.acct_number
        LEFT JOIN {{ ref('stg_lucid__accounts') }} AS to_account
            ON blotter.to_account = to_account.acct_number
    WHERE
        COALESCE(blotter.from_account,0) != COALESCE(blotter.to_account,0)
        AND
        COALESCE(from_account.fund, to_account.fund) IS NOT NULL
)

SELECT
  *
FROM
  final
