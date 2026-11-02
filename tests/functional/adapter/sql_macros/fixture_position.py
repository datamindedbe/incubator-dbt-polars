# position

seeds__data_position_csv = """substring_text,string_text,result
def,abcdef,4
land,earth,0
town,fishtown,5
ember,december,4
"""


models__test_position_sql = """
with data as (

    select * from {{ ref('data_position') }}

)

select

    {{ position('substring_text', 'string_text') }} as actual,
    result as expected

from data
"""


models__test_position_literals_sql = """
{% set searches = [("'.'", 2), ("'?'", 4), ("'+'", 6), ("'it''s'", 8), ("'z'", 0)] %}
{% for search, expected in searches %}
select {{ position(search, "'a.b?c+dit''s'") }} as actual, {{ expected }} as expected
{% if not loop.last %}union all{% endif %}
{% endfor %}
"""


models__test_position_yml = """
version: 2
models:
  - name: test_position
    data_tests:
      - assert_equal:
          actual: actual
          expected: expected
"""
