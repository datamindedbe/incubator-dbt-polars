{% macro polars__listagg(measure, delimiter_text="','", order_by_clause=none, limit_num=none) -%}
    {%- if order_by_clause or limit_num -%}
{{ exceptions.raise_compiler_error(
    "listagg() with an explicit order or a limit is not supported by dbt-polars: "
    "Polars' SQL engine has no WITHIN GROUP clause (needed for ordering) and no "
    "array-slicing function (needed to bound the result to N values), so this "
    "can't be expressed as a single macro call. Use a Python model instead."
) }}
    {%- else -%}
listagg({{ measure }}, {{ delimiter_text }})
    {%- endif -%}
{%- endmacro %}
