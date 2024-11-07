WITH source AS (
  SELECT
      DISTINCT
        *
  FROM
    {{ source(
      'lucid',
      'cash_and_security_transactions'
    ) }}
),
renamed AS (
  SELECT
    cash_account_number,
    cash_account_name,
    client_reference_number,
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
    status,
    detailed_transaction_status,
    transaction_type_name,
    detail_tran_type_description,
    local_currency_code,
    TRY_CAST(
      [settle_/_pay_date] AS DATE
    ) AS settle_pay_date,
    TRY_CAST(
      actual_settle_date AS DATE
    ) AS actual_settle_date,
    TRY_CAST(
      cash_value_date AS DATE
    ) AS cash_value_date,
    TRY_CAST(
      local_amount AS money
    ) AS local_amount,
    TRY_CAST(
      [shares_/_par] AS money
    ) AS shares_par,
    [cusip/cins] AS cusip_cins,
    isin,
    sweep_vehicle_name,
    location_name,
    reference_number,
    TRY_CAST(
      cash_post_date AS DATE
    ) AS cash_post_date
  FROM
    source
  WHERE transaction_type_name != 'INTERNAL MOVEMENT'
),
final AS (
  SELECT
    GREATEST(actual_settle_date,settle_pay_date,cash_value_date,cash_post_date) AS report_date,
    SUBSTRING(cash_account_number,0,7) AS short_acct_number,
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
