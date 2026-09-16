# dbt-polars-specific: unlike upstream's fixture_safe_cast.py (which only casts to
# string, so never exercises failure), this checks that an invalid cast actually
# returns null instead of raising, via Polars' native try_cast.

seeds__data_safe_cast_invalid_csv = """field,expected
abc,
123,123
,
"""


models__test_safe_cast_invalid_sql = """
with data as (

    select * from {{ ref('data_safe_cast_invalid') }}

)

select
    {{ safe_cast('field', api.Column.translate_type('integer')) }} as actual,
    expected

from data
"""


models__test_safe_cast_invalid_yml = """
version: 2
models:
  - name: test_safe_cast_invalid
    data_tests:
      - assert_equal:
          actual: actual
          expected: expected
"""
