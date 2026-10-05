{# Polars < 2.0 can't cast a string with a space-separated time to a timestamp, so
   quoted ISO literals become typed literals; any other expression is returned as is. #}
{% macro polars__typed_temporal_literal(expression) -%}
    {%- set text = expression | string | trim -%}
    {%- if modules.re.fullmatch("'[0-9]{4}-[0-9]{2}-[0-9]{2}'", text) -%}
        {{ return("DATE " ~ text) }}
    {%- elif modules.re.fullmatch("'[0-9]{4}-[0-9]{2}-[0-9]{2}[ T][0-9:.]+'", text) -%}
        {{ return("TIMESTAMP " ~ text) }}
    {%- else -%}
        {{ return(expression) }}
    {%- endif -%}
{%- endmacro %}
