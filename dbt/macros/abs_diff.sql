{% macro abs_diff(val1, val2) %}
  (ABS(ABS({{ val1 }}) - ABS({{ val2 }})))
{% endmacro %}
