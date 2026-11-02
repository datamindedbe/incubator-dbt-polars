{% macro polars__hash(field) -%}
    {%- if execute -%}
    {{ exceptions.raise_compiler_error(
        "hash() is not supported by dbt-polars: Polars' SQL engine exposes no "
        "hashing function (md5, sha256, or otherwise). Use a Python model instead."
    ) }}
    {%- else -%}
null
    {%- endif -%}
{%- endmacro %}
