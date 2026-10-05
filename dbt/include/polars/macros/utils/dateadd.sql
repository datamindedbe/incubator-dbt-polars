{% macro polars__dateadd(datepart, interval, from_date_or_timestamp) -%}
    {%- set unit = datepart | lower -%}
    {%- set source = "(" ~ polars__typed_temporal_literal(from_date_or_timestamp) ~ ")" -%}
    {%- set interval_text = interval | string | trim -%}
    {%- set sub_day_units = ['microsecond', 'millisecond', 'second', 'minute', 'hour'] -%}
    {%- set months_per_unit = {'month': 1, 'quarter': 3, 'year': 12} -%}
    {%- if unit not in sub_day_units and unit not in ['day', 'week'] and unit not in months_per_unit -%}
        {{ exceptions.raise_compiler_error("dateadd() does not support datepart '" ~ datepart ~ "' on dbt-polars.") }}
    {%- endif -%}
    {#- Adding hours to a date would silently stay a date, so sub-day units work on timestamps. -#}
    {%- if unit in sub_day_units -%}
        {%- set source = "cast(" ~ source ~ " as timestamp)" -%}
    {%- endif -%}
    {%- if modules.re.fullmatch('-?[0-9]+', interval_text) -%}
        {%- set count = interval_text | int -%}
({{ source }} {{ '-' if count < 0 else '+' }} INTERVAL '{{ count | abs }} {{ unit }}')
    {%- elif unit in months_per_unit -%}
{{ polars__dateadd_months(source, "(" ~ interval ~ ") * " ~ months_per_unit[unit]) }}
    {%- else -%}
({{ source }} + INTERVAL '1 {{ unit }}' * ({{ interval }}))
    {%- endif -%}
{%- endmacro %}


{% macro polars__dateadd_months(source, months_sql) -%}
    {%- set source_date = "cast(" ~ source ~ " as date)" -%}
    {%- set month_index = "(date_part('year', " ~ source_date ~ ") * 12 + date_part('month', " ~ source_date ~ ") - 1 + " ~ months_sql ~ ")" -%}
    {%- set year_sql = "cast(floor(" ~ month_index ~ " / 12) as bigint)" -%}
    {%- set month_sql = "cast(" ~ month_index ~ " - floor(" ~ month_index ~ " / 12) * 12 + 1 as bigint)" -%}
    {%- set first_of_month = "try_cast(concat(cast(" ~ year_sql ~ " as varchar), '-', lpad(cast(" ~ month_sql ~ " as varchar), 2, '0'), '-01') as date)" -%}
    {%- set last_day_of_month = "date_part('day', " ~ first_of_month ~ " + INTERVAL '1 month' - INTERVAL '1 day')" -%}
    {%- set target_days = "(cast(" ~ first_of_month ~ " as bigint) + least(date_part('day', " ~ source_date ~ "), " ~ last_day_of_month ~ ") - 1)" -%}
({{ source }} + INTERVAL '1 day' * ({{ target_days }} - cast({{ source_date }} as bigint)))
{%- endmacro %}
