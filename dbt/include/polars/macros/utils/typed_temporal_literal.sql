{# Polars < 2.0 can't cast a string with a space-separated time to a timestamp, so
   quoted ISO literals become typed literals; any other expression is returned as is.
   Polars can't parse a zone in a literal, so a zoned timestamp is shifted to UTC. #}
{% macro polars__typed_temporal_literal(expression) -%}
    {%- set text = expression | string | trim -%}
    {%- set zoned = modules.re.fullmatch("'([0-9]{4}-[0-9]{2}-[0-9]{2}[ T][0-9:.]+)(Z|([+-])([0-9]{2}):?([0-9]{2}))'", text) -%}
    {%- if modules.re.fullmatch("'[0-9]{4}-[0-9]{2}-[0-9]{2}'", text) -%}
        {{ return("DATE " ~ text) }}
    {%- elif modules.re.fullmatch("'[0-9]{4}-[0-9]{2}-[0-9]{2}[ T][0-9:.]+'", text) -%}
        {{ return("TIMESTAMP " ~ text) }}
    {%- elif zoned and zoned.group(2) == 'Z' -%}
        {{ return("TIMESTAMP '" ~ zoned.group(1) ~ "'") }}
    {%- elif zoned -%}
        {%- set offset_minutes = (zoned.group(4) | int) * 60 + (zoned.group(5) | int) -%}
        {{ return("(TIMESTAMP '" ~ zoned.group(1) ~ "' " ~ ('-' if zoned.group(3) == '+' else '+') ~ " INTERVAL '" ~ offset_minutes ~ " minute')") }}
    {%- else -%}
        {{ return(expression) }}
    {%- endif -%}
{%- endmacro %}
