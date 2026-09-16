{% macro polars__date_trunc(datepart, date) -%}
    {%- set part = datepart | lower -%}
    {%- if part == 'day' -%}
cast(cast({{ date }} as date) as timestamp)
    {%- elif part in ('month', 'year') -%}
cast(cast(strftime(cast({{ date }} as timestamp), '{{ "%Y-%m-01" if part == "month" else "%Y-01-01" }}') as date) as timestamp)
    {%- elif part in ('hour', 'minute') -%}
cast(strftime(cast({{ date }} as timestamp), '{{ "%Y-%m-%dT%H:00:00" if part == "hour" else "%Y-%m-%dT%H:%M:00" }}') as timestamp)
    {%- else -%}
{{ exceptions.raise_compiler_error(
    "date_trunc('" ~ datepart ~ "', ...) is not supported by dbt-polars: only "
    ~ "day/hour/minute/month/year truncation is implemented. Polars' SQL engine "
    ~ "has no native DATE_TRUNC function; this override only covers dateparts "
    ~ "that can be built from CAST/STRFTIME."
) }}
    {%- endif -%}
{%- endmacro %}
