{# Casting a timezone-aware timestamp to date yields its UTC date. Its text form starts
   with the local date, as does that of a date, naive timestamp or ISO string. #}
{% macro polars__local_date(expression) -%}
    cast(left(cast({{ expression }} as varchar), 10) as date)
{%- endmacro %}
