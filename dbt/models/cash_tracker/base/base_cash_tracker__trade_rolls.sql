{{
    config({
        "as_columnstore": false,
        "materialized": 'table',
        "post-hook": [
            "{{ create_clustered_index(columns = ['report_date', 'roll_of'], unique=True) }}",
        ]
    })

}}

WITH
trades AS (
  SELECT
    *,
    quantity AS par
  FROM {{ ref('cash_tracker__trade_query_on_date') }}
),
rolling_on_by_date AS (
  SELECT
    report_date,
    roll_of
  FROM trades
  WHERE 
    (series = 'MASTER' OR is_also_master = 1)
    AND report_date = start_date
    AND roll_of != 0
    AND roll_of IS NOT NULL
),
rolling_on_static AS (
  SELECT
    report_date,
    roll_of
  FROM (values 
      (null,'131509'),
      (null,'131510'),
      (null,'131803'),
      (null,'131807'),
      (null,'131811'),
      (null,'132166'),
      (null,'132167'),
      (null,'132175'),
      (null,'132185'),
      (null,'134690'),
      (null,'134887'),
      (null,'134888'),
      (null,'134889'),
      (null,'135076')
  ) as t(report_date,roll_of)
),
final AS (
  SELECT
    report_date,
    roll_of
  FROM rolling_on_by_date 
  UNION 
  SELECT
    report_date,
    roll_of
  FROM rolling_on_static 
)

SELECT * FROM final
