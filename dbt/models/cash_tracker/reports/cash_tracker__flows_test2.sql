{{ config(
  enabled=false
)}}

WITH 
expected AS (
  SELECT
    report_date,
    fund,
    flow_account AS acct_name,
    flow_acct_number AS acct_number,
    related_helix_id AS helix_id,
    transaction_action_id,
    transaction_desc,
    expected_is_settled AS settled,
    flow_amount AS amount,
    margin_total,
    is_margin,
    counterparty,
    is_po,
    match_rank,
    reference_number,
    transaction_type_name,
    generated_id,
    [route]
  FROM {{ ref('cash_tracker__expected_flows') }}
),
observed AS (
  SELECT
    report_date,
    fund,
    acct_name,
    short_acct_number AS acct_number,
    helix_id,
    client_reference_number,
    local_amount AS amount,
    reference_number,
    transaction_type_name
  FROM {{ ref('stg_lucid__cash_and_security_transactions') }}
  WHERE 1=1
  AND (location_name != 'STIF LOCATIONS' OR transaction_type_name = 'DIVIDEND')
),
expected_flows AS (
  SELECT
    'EXPECTED' AS source,
    report_date,
    fund,
    acct_name,
    acct_number,
    helix_id,
    transaction_desc,
    settled,
    amount,
    margin_total,
    CASE
      WHEN is_margin = 1 THEN margin_total
      ELSE amount
    END AS sort_amount,
    reference_number,
    [route],
    match_rank,
    generated_id
  FROM expected
),
observed_flows AS (
  SELECT
    'OBSERVED' AS source,
    report_date,
    fund,
    acct_name,
    acct_number,
    helix_id,
    client_reference_number,
    amount,
    reference_number
  FROM observed
),
combined AS (
  SELECT
    COALESCE(e.report_date, o.report_date) AS report_date,
    COALESCE(e.fund, o.fund) AS fund,
    COALESCE(e.acct_name, o.acct_name) AS acct_name,
    e.helix_id,
    e.settled,
    e.[route],
    e.match_rank,
    e.transaction_desc AS e_desc,
    e.margin_total,
    e.amount AS e_amount,
    o.amount AS o_amount,
    o.client_reference_number AS o_desc,
    COALESCE(e.reference_number, o.reference_number) AS reference_number,
    ROW_NUMBER() OVER (ORDER BY COALESCE(e.sort_amount, o.amount), e.generated_id) AS row_num,
    CASE
    WHEN o.reference_number IS NULL THEN 0
    ELSE ROW_NUMBER() OVER (PARTITION BY o.report_date, o.fund, o.reference_number ORDER BY e.generated_id)
    END AS bnym_use
  FROM expected_flows AS e
  FULL OUTER JOIN observed_flows AS o 
    ON (
      e.report_date=o.report_date 
      AND e.fund = o.fund
      AND e.acct_name = o.acct_name
      AND e.reference_number=o.reference_number
    )
),
bnyn_summary AS (
  SELECT
    *
  FROM {{ ref('cash_tracker__bnym_summary')}}
),
balance_calc AS (
  SELECT
    c.report_date,
    c.fund,
    c.acct_name,
    c.helix_id,
    c.settled,
    c.[route],
    c.match_rank,
    c.margin_total,
    c.e_desc,
    COALESCE(SUM(c.e_amount) OVER (PARTITION BY c.report_date, c.fund, c.acct_name ORDER BY c.row_num),0) AS e_balance,
    c.e_amount,
    CASE
      WHEN bnym_use = 1 THEN c.o_amount
      ELSE NULL
    END AS o_amount,
    COALESCE(SUM(
      CASE WHEN bnym_use = 1 THEN c.o_amount ELSE NULL END
      ) OVER (PARTITION BY c.report_date, c.fund, c.acct_name ORDER BY c.row_num),0) AS o_balance,
    c.o_desc,
    c.reference_number,
    row_num AS order_by_amt,
    bnym_use
  FROM combined AS c
  LEFT JOIN bnyn_summary AS b
    ON (
      c.report_date=b.report_date
      AND c.fund = b.fund
      AND c.acct_name = b.acct_name
      AND b.security = '{{var('CASH')}}'
    )
),
final AS (
  SELECT
    report_date,
    fund,
    acct_name,
    helix_id,
    settled,
    [route],
    match_rank,
    margin_total,
    e_desc,
    e_balance,
    e_amount,
    {{ abs_diff('e_balance', 'o_balance') }} AS balance_diff,
    o_amount,
    o_balance,
    o_desc,
    reference_number,
    order_by_amt,
    bnym_use
  FROM balance_calc
)

SELECT * FROM final
