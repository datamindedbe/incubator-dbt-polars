{% macro polars__date(year, month, day) -%}
    {%- set dt = modules.datetime.date(year, month, day) -%}
DATE '{{ dt.strftime('%Y-%m-%d') }}'
{%- endmacro %}
