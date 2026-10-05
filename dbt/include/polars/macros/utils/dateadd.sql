{% macro polars__dateadd(datepart, interval, from_date_or_timestamp) -%}
    {%- set unit = datepart | lower -%}
    {%- set timestamp_sql = "cast(" ~ from_date_or_timestamp ~ " as timestamp)" -%}
    {%- set interval_text = interval | string | trim -%}
    {%- set fixed_length_units = ['microsecond', 'millisecond', 'second', 'minute', 'hour', 'day', 'week'] -%}
    {%- set months_per_unit = {'month': 1, 'quarter': 3, 'year': 12} -%}
    {%- if unit not in fixed_length_units and unit not in months_per_unit -%}
        {{ exceptions.raise_compiler_error("dateadd() does not support datepart '" ~ datepart ~ "' on dbt-polars.") }}
    {%- elif modules.re.fullmatch('-?[0-9]+', interval_text) -%}
        {%- set count = interval_text | int -%}
({{ timestamp_sql }} {{ '-' if count < 0 else '+' }} INTERVAL '{{ count | abs }} {{ unit }}')
    {%- elif unit in fixed_length_units -%}
({{ timestamp_sql }} + INTERVAL '1 {{ unit }}' * ({{ interval }}))
    {%- else -%}
{{ polars__dateadd_months(timestamp_sql, "(" ~ interval ~ ") * " ~ months_per_unit[unit]) }}
    {%- endif -%}
{%- endmacro %}


{% macro polars__dateadd_months(timestamp_sql, months_sql) -%}
    {%- set month_index = "(date_part('year', " ~ timestamp_sql ~ ") * 12 + date_part('month', " ~ timestamp_sql ~ ") - 1 + " ~ months_sql ~ ")" -%}
    {%- set year_sql = "cast(floor(" ~ month_index ~ " / 12) as bigint)" -%}
    {%- set month_sql = "cast(" ~ month_index ~ " - floor(" ~ month_index ~ " / 12) * 12 + 1 as bigint)" -%}
    {%- set first_of_month = "cast(try_cast(concat(cast(" ~ year_sql ~ " as varchar), '-', lpad(cast(" ~ month_sql ~ " as varchar), 2, '0'), '-01') as date) as timestamp)" -%}
    {%- set last_day_of_month = "date_part('day', " ~ first_of_month ~ " + INTERVAL '1 month' - INTERVAL '1 day')" -%}
    {%- set day_of_month = "least(date_part('day', " ~ timestamp_sql ~ "), " ~ last_day_of_month ~ ")" -%}
    {%- set time_of_day = "(" ~ timestamp_sql ~ " - cast(cast(" ~ timestamp_sql ~ " as date) as timestamp))" -%}
({{ first_of_month }} + INTERVAL '1 day' * ({{ day_of_month }} - 1) + {{ time_of_day }})
{%- endmacro %}
