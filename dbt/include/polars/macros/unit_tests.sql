{#- Overrides dbt-core's get_fixture_sql. The fixture becomes an ephemeral Python model
    that casts the rows to the Polars schema of the input. -#}
{% macro get_fixture_sql(rows, column_name_to_data_types) %}
{%- set input_relation = adapter.polars_resolve_relation(this) -%}
{%- set this_or_defer_relation = defer_relation if (defer_relation and not load_relation(input_relation)) else input_relation -%}
{%- set columns_in_relation = adapter.get_columns_in_relation(this_or_defer_relation) -%}
{%- set column_name_to_data_types = {} -%}
{%- for column in columns_in_relation -%}
{%-   do column_name_to_data_types.update({column.name|lower: column.data_type}) -%}
{%- endfor -%}

{%- if not column_name_to_data_types -%}
    {{ exceptions.raise_compiler_error("Not able to get columns for unit test '" ~ model.name ~ "' from relation " ~ this ~ " because the relation doesn't exist") }}
{%- endif -%}

{{ validate_fixture_rows(rows, row_number) }}
{%- for row in rows -%}
{%-   do format_row(row, column_name_to_data_types) -%}
{%- endfor %}
import json

from polars.datatypes import *

from dbt.adapters.polars.fixtures import fixture_frame


def model(dbt, session):
    rows = json.loads({{ tojson(tojson(rows)) }})
    schema = {
{%- for column in columns_in_relation %}
        {{ tojson(column.name) }}: {{ column.data_type }},
{%- endfor %}
    }
    return fixture_frame(rows, schema)


{{ py_script_postfix(model) }}
{% endmacro %}


{#- dbt-core's py_script_postfix for a unit test on a Python model. In a unit test
    `this` is none, or the name of the `this` fixture CTE, instead of a relation. -#}
{% macro polars_unit_test_python_postfix(model) %}
{{ build_ref_function(model) }}
{{ build_source_function(model) }}
{{ build_config_dict(model) }}

class config:
    def __init__(self, *args, **kwargs):
        pass

    @staticmethod
    def get(key, default=None):
        return config_dict.get(key, default)

    @staticmethod
    def meta_get(key, default=None):
        return meta_dict.get(key, default)

class this:
    """dbt.this() or dbt.this.identifier"""
    database = None
    schema = None
    identifier = "{{ this or '' }}"
    def __repr__(self):
        return "{{ this or '' }}"

class dbtObj:
    def __init__(self, load_df_function) -> None:
        self.source = lambda *args: source(*args, dbt_load_df_function=load_df_function)
        self.ref = lambda *args, **kwargs: ref(*args, **kwargs, dbt_load_df_function=load_df_function)
        self.config = config
        self.this = this()
        {#- is_incremental() reads this, and is false unless the unit test overrides it #}
        self.is_incremental = {{ is_incremental() if (model.overrides and 'is_incremental' in model.overrides.macros) else False }}
{% endmacro %}


{% materialization unit, adapter='polars' %}
  {%- set expected_rows = config.get('expected_rows') -%}
  {%- set expected_sql = config.get('expected_sql') -%}

  {%- set column_name_to_data_types = {} -%}
  {%- for column in adapter.polars_get_columns_in_unit_test(sql, model['extra_ctes']) -%}
  {%-   do column_name_to_data_types.update({column.name|lower: column.data_type}) -%}
  {%- endfor -%}
  {%- for row in expected_rows -%}
  {%-   do format_row(row, column_name_to_data_types) -%}
  {%- endfor -%}

  {% set res, table = adapter.polars_execute_unit_test(sql, model['extra_ctes'], expected_rows, expected_sql) %}
  {{ store_result('main', response=res, agate_table=table) }}

  {{ return({'relations': []}) }}
{% endmaterialization %}
