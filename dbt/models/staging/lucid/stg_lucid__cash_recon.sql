WITH source AS (
  SELECT
    *
  FROM
    {{ source(
      'lucid',
      'cash_recon'
    ) }}
),
renamed AS (
  SELECT
    cash_account_number,
    cash_account_name,
    client_reference_number,
    status,
    detailed_transaction_status,
    transaction_type_name,
    detail_tran_type_description,
    local_currency_code,
    TRY_CAST(
      settle_pay_date AS DATE
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
      shares_par AS money
    ) AS shares_par,
    cusip_cins,
    isin,
    sweep_vehicle_name,
    location_name,
    reference_number,
    TRY_CAST(
      cash_post_date AS DATE
    ) AS cash_post_date
  FROM
    source
)
SELECT
  *
FROM
  renamed
