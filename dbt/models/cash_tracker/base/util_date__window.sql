{{
    config(
        materialized='table'
    )
}}

WITH date_dimension AS (
  {{ dim_date_transact(
    '2020-01-01',
    '2025-01-01'
  ) }}
)
SELECT
  *
FROM
  date_dimension;
