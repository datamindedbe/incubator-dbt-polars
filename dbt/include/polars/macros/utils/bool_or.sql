{% macro polars__bool_or(expression) -%}
    max(cast(({{ expression }}) as boolean))
{%- endmacro %}
