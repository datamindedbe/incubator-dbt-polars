{% macro polars__hash(field) -%}
    cast({{ field }} as varchar)
{%- endmacro %}

{% macro polars__md5(field) -%}
    cast({{ field }} as varchar)
{%- endmacro %}

{% macro polars__date_trunc(datepart, date) -%}
    {%- set datepart_lower = datepart | lower -%}
    {%- if datepart_lower == 'year' -%}
        CAST(STRFTIME({{ date }}, '%Y-01-01') AS DATE)
    {%- elif datepart_lower == 'quarter' -%}
        CASE
            WHEN EXTRACT(month FROM {{ date }}) <= 3 THEN CAST(STRFTIME({{ date }}, '%Y-01-01') AS DATE)
            WHEN EXTRACT(month FROM {{ date }}) <= 6 THEN CAST(STRFTIME({{ date }}, '%Y-04-01') AS DATE)
            WHEN EXTRACT(month FROM {{ date }}) <= 9 THEN CAST(STRFTIME({{ date }}, '%Y-07-01') AS DATE)
            ELSE CAST(STRFTIME({{ date }}, '%Y-10-01') AS DATE)
        END
    {%- elif datepart_lower == 'month' -%}
        CAST(STRFTIME({{ date }}, '%Y-%m-01') AS DATE)
    {%- elif datepart_lower == 'week' -%}
        {{ date }} - (CASE WHEN EXTRACT(dow FROM {{ date }}) = 0 THEN 6 ELSE (EXTRACT(dow FROM {{ date }}) - 1)::INT END) * INTERVAL '1 day'
    {%- elif datepart_lower == 'day' -%}
        CAST(STRFTIME({{ date }}, '%Y-%m-%d') AS DATE)
    {%- else -%}
        {{ exceptions.raise_compiler_error("polars__date_trunc: unsupported datepart '" ~ datepart ~ "'") }}
    {%- endif -%}
{%- endmacro %}
