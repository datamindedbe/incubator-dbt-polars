# date

models__test_date_sql = """
with generated_dates as (

    {{
        dbt.date_spine(
            "day",
            date(2023, 9, 7),
            date(2023, 9, 10),
        )
    }}

),

expected_dates as (

    select cast('2023-09-07' as date) as expected
    union all
    select cast('2023-09-08' as date) as expected
    union all
    select cast('2023-09-09' as date) as expected

),

joined as (
    select
        generated_dates.date_day,
        expected_dates.expected
    from generated_dates
    full outer join expected_dates on generated_dates.date_day = expected_dates.expected
)

select * from joined
"""

models__test_date_yml = """
version: 2
models:
  - name: test_date
    data_tests:
      - assert_equal:
          actual: date_day
          expected: expected
"""


# dbt-polars-specific: upstream's own fixture only ever exercises `date()` inside
# `date_spine()`, which is separately unsupported (see test_date_spine.py). This
# checks `date()` on its own so the macro implemented here actually has coverage.
models__test_date_standalone_sql = """
select
    {{ date(2023, 9, 7) }} as actual,
    cast('2023-09-07' as date) as expected
"""

models__test_date_standalone_yml = """
version: 2
models:
  - name: test_date_standalone
    data_tests:
      - assert_equal:
          actual: actual
          expected: expected
"""
