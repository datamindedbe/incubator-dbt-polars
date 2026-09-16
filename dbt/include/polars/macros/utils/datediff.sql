{% macro polars__datediff(first_date, second_date, datepart) -%}
    {{ exceptions.raise_compiler_error(
        "datediff() is not supported by dbt-polars: Polars' SQL engine has no "
        "datediff()/date_diff() function. Two dates can be subtracted directly "
        "(yielding a duration), but there is no SQL-level way to count an arbitrary "
        "date part (day/week/month/year/...) between them the way datediff() "
        "requires. Use a Python model instead."
    ) }}
{%- endmacro %}
