{% macro prior_business_day(input_date) %}
(
  SELECT MAX(calendar_date)
  FROM {{ ref('stg_lucid__calendar') }}
  WHERE is_business_day = 1 AND calendar_date < {{ input_date }}
)
{% endmacro %}