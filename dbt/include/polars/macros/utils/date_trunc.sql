{% macro polars__date_trunc(datepart, date) -%}
    {%- set part = datepart | lower -%}
    {%- set date = polars__typed_temporal_literal(date) -%}
    {%- set local_date = polars__local_date(date) -%}
    {%- if part == 'day' -%}
cast({{ local_date }} as timestamp)
    {%- elif part == 'week' -%}
cast({{ local_date }} - INTERVAL '1 day' * ((date_part('dow', {{ local_date }}) + 6) % 7) as timestamp)
    {%- elif part == 'quarter' -%}
cast(try_cast(concat(strftime({{ local_date }}, '%Y'), '-', lpad(cast(cast(floor((date_part('month', {{ local_date }}) - 1) / 3) * 3 + 1 as bigint) as varchar), 2, '0'), '-01') as date) as timestamp)
    {%- elif part in ('month', 'year') -%}
cast(cast(strftime({{ local_date }}, '{{ "%Y-%m-01" if part == "month" else "%Y-01-01" }}') as date) as timestamp)
    {%- elif part in ('hour', 'minute') -%}
cast(strftime(cast({{ date }} as timestamp), '{{ "%Y-%m-%dT%H:00:00" if part == "hour" else "%Y-%m-%dT%H:%M:00" }}') as timestamp)
    {%- else -%}
{{ exceptions.raise_compiler_error(
    "date_trunc('" ~ datepart ~ "', ...) is not supported by dbt-polars: only "
    ~ "minute/hour/day/week/month/quarter/year truncation is implemented. Polars' SQL engine "
    ~ "has no native DATE_TRUNC function; this override only covers dateparts "
    ~ "that can be built from CAST/STRFTIME."
) }}
    {%- endif -%}
{%- endmacro %}
