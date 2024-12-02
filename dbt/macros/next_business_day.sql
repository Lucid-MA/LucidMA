{% macro next_business_day(input_date) %}
(
  SELECT MIN(calendar_date)
  FROM {{ ref('stg_lucid__calendar') }}
  WHERE is_business_day = 1 AND calendar_date > {{ input_date }}
)
{% endmacro %}