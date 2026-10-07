{# Casting a timezone-aware timestamp to date yields its UTC date; formatting it first keeps the local date. #}
{% macro polars__local_date(expression) -%}
    cast(strftime({{ expression }}, '%Y-%m-%d') as date)
{%- endmacro %}
