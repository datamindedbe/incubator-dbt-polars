{% macro polars__any_value(expression) -%}
    first({{ expression }})
{%- endmacro %}
