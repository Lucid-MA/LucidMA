{% macro abs_diff(val1, val2) %}
  CASE
    WHEN {{ val1 }} < {{ val2 }} THEN ABS({{ val2 }} - {{ val1 }})
    ELSE ABS({{ val1 }} - {{ val2 }})
  END
{% endmacro %}
