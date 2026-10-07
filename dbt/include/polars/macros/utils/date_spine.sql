{# dbt-core's default__date_spine without its row_number() window: generate_series
   yields 1..n, so generated_number - 1 is already the offset. #}
{% macro polars__date_spine(datepart, start_date, end_date) %}

    with rawdata as (

        {{dbt.generate_series(
            dbt.get_intervals_between(start_date, end_date, datepart)
        )}}

    ),

    all_periods as (

        select (
            {{
                dbt.dateadd(
                    datepart,
                    "generated_number - 1",
                    start_date
                )
            }}
        ) as date_{{datepart}}
        from rawdata

    ),

    filtered as (

        select *
        from all_periods
        where date_{{datepart}} <= {{ polars__typed_temporal_literal(end_date) }}

    )

    select * from filtered

{% endmacro %}
