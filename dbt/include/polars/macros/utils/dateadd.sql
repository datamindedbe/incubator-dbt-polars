{% macro polars__dateadd(datepart, interval, from_date_or_timestamp) -%}
    {{ exceptions.raise_compiler_error(
        "dateadd() is not supported by dbt-polars: Polars' SQL engine has no "
        "dateadd()/date_add() function and no INTERVAL literal syntax, so there is "
        "no SQL-level way to add a date/time offset. Use a Python model instead."
    ) }}
{%- endmacro %}
