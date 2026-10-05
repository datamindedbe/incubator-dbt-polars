{% macro polars__date_trunc(datepart, date) -%}
    {%- set part = datepart | lower -%}
    {%- if part == 'day' -%}
cast(cast({{ date }} as date) as timestamp)
    {%- elif part == 'week' -%}
        {%- set day_sql = "cast(" ~ date ~ " as date)" -%}
cast({{ day_sql }} - INTERVAL '1 day' * ((date_part('dow', {{ day_sql }}) + 6) % 7) as timestamp)
    {%- elif part == 'quarter' -%}
        {%- set timestamp_sql = "cast(" ~ date ~ " as timestamp)" -%}
cast(try_cast(concat(strftime({{ timestamp_sql }}, '%Y'), '-', lpad(cast(cast(floor((date_part('month', {{ timestamp_sql }}) - 1) / 3) * 3 + 1 as bigint) as varchar), 2, '0'), '-01') as date) as timestamp)
    {%- elif part in ('month', 'year') -%}
cast(cast(strftime(cast({{ date }} as timestamp), '{{ "%Y-%m-01" if part == "month" else "%Y-01-01" }}') as date) as timestamp)
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
