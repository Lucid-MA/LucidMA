{{
    config(
        materialized='table'
    )
}}

WITH 
date_dimension AS (
  {{ dim_date_transact(
    '2018-09-01',
    '2026-01-01'
  ) }}
),
calendar AS (
  SELECT
    d AS calendar_date
  FROM date_dimension
),
base_calendar AS (
  SELECT calendar_date
  FROM calendar
),
holidays AS (
  SELECT date AS holiday_date
  FROM {{ source('sql2', 'holidays') }}
),
final AS (
  SELECT
    c.calendar_date,
    CASE
      WHEN DATENAME(WEEKDAY, c.calendar_date) IN ('Saturday', 'Sunday') THEN 0
      WHEN h.holiday_date IS NOT NULL THEN 0
      ELSE 1
    END AS is_business_day
  FROM base_calendar c
  LEFT JOIN holidays h
    ON c.calendar_date = h.holiday_date
)

SELECT * FROM final
