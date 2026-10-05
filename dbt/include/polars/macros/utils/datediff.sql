{% macro polars__datediff(first_date, second_date, datepart) -%}
    {%- set first_date = polars__typed_temporal_literal(first_date) -%}
    {%- set second_date = polars__typed_temporal_literal(second_date) -%}
    {%- set unit = datepart | lower -%}
    {%- set microseconds_per_unit = {'microsecond': 1, 'millisecond': 1000, 'second': 1000000, 'minute': 60000000, 'hour': 3600000000} -%}
    {%- set first_day = "cast(cast(" ~ first_date ~ " as date) as bigint)" -%}
    {%- set second_day = "cast(cast(" ~ second_date ~ " as date) as bigint)" -%}
    {%- set year_diff = "(date_part('year', cast(" ~ second_date ~ " as date)) - date_part('year', cast(" ~ first_date ~ " as date)))" -%}
    {%- if unit == 'day' -%}
cast({{ second_day }} - {{ first_day }} as bigint)
    {%- elif unit == 'week' -%}
    {#- 1970-01-01 was a Thursday; +4 days aligns the weeks to start on Sunday. -#}
cast(floor(({{ second_day }} + 4) / 7) - floor(({{ first_day }} + 4) / 7) as bigint)
    {%- elif unit == 'month' -%}
cast({{ year_diff }} * 12 + date_part('month', cast({{ second_date }} as date)) - date_part('month', cast({{ first_date }} as date)) as bigint)
    {%- elif unit == 'quarter' -%}
cast({{ year_diff }} * 4 + date_part('quarter', cast({{ second_date }} as date)) - date_part('quarter', cast({{ first_date }} as date)) as bigint)
    {%- elif unit == 'year' -%}
cast({{ year_diff }} as bigint)
    {%- elif unit in microseconds_per_unit -%}
        {%- set per_unit = microseconds_per_unit[unit] -%}
cast(floor(cast(cast({{ second_date }} as timestamp) as bigint) / {{ per_unit }}) - floor(cast(cast({{ first_date }} as timestamp) as bigint) / {{ per_unit }}) as bigint)
    {%- else -%}
        {{ exceptions.raise_compiler_error("datediff() does not support datepart '" ~ datepart ~ "' on dbt-polars.") }}
    {%- endif -%}
{%- endmacro %}
