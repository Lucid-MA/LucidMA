{{
    config({
        "as_columnstore": false,
        "materialized": 'table',
        "post-hook": [
            "{{ create_nonclustered_index(columns = ['report_date']) }}",
            "{{ create_nonclustered_index(columns = ['short_acct_number']) }}",
            "{{ create_nonclustered_index(columns = ['fund']) }}",
            "{{ create_nonclustered_index(columns = ['acct_name']) }}",
        ]
    })
}}

WITH source AS (
  SELECT
      DISTINCT
        *
  FROM
    {{ source(
      'sql2',
      'bronze_nexen_cash_and_security_transactions'
    ) }}
),
renamed AS (
  SELECT
    TRIM([Cash Account Number]) AS cash_account_number,
    TRIM([Cash Account Name]) AS cash_account_name,
    TRIM([Client Reference Number]) AS client_reference_number,
    [status],
    TRIM([Detailed Transaction Status]) AS detailed_transaction_status,
    TRIM([Transaction Type Name]) AS transaction_type_name,
    TRIM([Detail Tran Type Description]) AS detail_tran_type_description,
    [Local Currency Code] AS local_currency_code,
    TRY_CAST(
      [Settle / Pay Date] AS DATE
    ) AS settle_pay_date,
    TRY_CAST(
      [Actual Settle Date] AS DATE
    ) AS actual_settle_date,
    TRY_CAST(
      [Cash Value Date] AS DATE
    ) AS cash_value_date,
    TRY_CAST(
      [Local Amount] AS money
    ) AS local_amount,
    TRY_CAST(
      [Shares / Par] AS money
    ) AS shares_par,
    [CUSIP/CINS] AS cusip_cins,
    isin,
    TRIM([Sweep Vehicle Name]) AS sweep_vehicle_name,
    TRIM([Location Name]) AS location_name,
    TRIM([Reference Number]) AS reference_number,
    TRY_CAST(
      [Cash Post Date] AS DATE
    ) AS cash_post_date,
    [Transaction Status Post Timestamp] AS transaction_status_post_timestamp,
    [timestamp] AS created_at
  FROM
    source
),
final AS (
  SELECT
    GREATEST(actual_settle_date,settle_pay_date,cash_value_date,cash_post_date) AS report_date,
    TRY_CAST(SUBSTRING(cash_account_number,0,7) AS VARCHAR(10)) AS short_acct_number,
     CASE
      WHEN PATINDEX('%[^0-9]%', client_reference_number) = 0 THEN TRY_CAST(client_reference_number AS INT)
      WHEN PATINDEX('%[0-9]%', client_reference_number) > 0 THEN
        TRY_CAST(
          LEFT(
            SUBSTRING(client_reference_number, PATINDEX('%[0-9]%', client_reference_number), LEN(client_reference_number)),
            PATINDEX('%[^0-9]%', SUBSTRING(client_reference_number, PATINDEX('%[0-9]%', client_reference_number), LEN(client_reference_number)) + 'X') - 1
          ) AS INT
        )
      ELSE NULL
    END AS helix_id,
    CASE
      WHEN UPPER(SUBSTRING(client_reference_number,1,7)) = 'HXSWING' THEN 1
      ELSE 0 
    END AS is_hxswing,
    CASE
      WHEN location_name = 'STIF LOCATIONS' AND transaction_type_name != 'DIVIDEND' THEN 1
      ELSE 0
    END AS sweep_detected,
    *
  FROM renamed
)
SELECT
  final.*,
  a.fund,
  a.acct_name
FROM
  final
LEFT JOIN {{ ref('stg_lucid__accounts')}} AS a
  ON (final.short_acct_number = a.acct_number)
WHERE TRIM(UPPER(transaction_type_name)) != 'INTERNAL MOVEMENT'
  AND cash_account_number LIKE '%8400'
