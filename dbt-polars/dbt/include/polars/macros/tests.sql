{% materialization test, adapter='polars' %}
  {%- set language = model['language'] -%}
  {% set relations = [] %}

  {% if language == 'python' %}

    {% set limit = config.get('limit') %}
    {% set fail_calc = config.get('fail_calc') %}
    {% set warn_if = config.get('warn_if') %}
    {% set error_if = config.get('error_if') %}


    {% set target_relation = none %}
    {% if should_store_failures() %}
      {% set identifier = model['alias'] %}
      {% set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) %}
      {% set store_failures_as = config.get('store_failures_as') %}
      {% if store_failures_as == none %}{% set store_failures_as = 'table' %}{% endif %}
      {% if store_failures_as != 'table' %}
        {{ exceptions.raise_compiler_error(
          "store_failures_as='" ~ store_failures_as ~ "' is not supported for Python singular tests on the "
          "polars adapter (views are not supported by this adapter; only 'table' is available)."
        ) }}
      {% endif %}
      {% set target_relation = api.Relation.create(
          identifier=identifier, schema=schema, database=database, type='table') -%}
      {% if old_relation %}{% do adapter.drop_relation(old_relation) %}{% endif %}
      {% do relations.append(target_relation) %}
    {% endif %}

    {% set full_code = compiled_code ~ "\n\n" ~ py_script_postfix(model) %}
    {{ write(full_code) }}
    {% set res, table = adapter.execute_python_test(model, full_code, limit, fail_calc, warn_if, error_if, target_relation) %}
    {% if target_relation is not none %}{{ adapter.commit() }}{% endif %}
    {{ store_result('main', response=res, agate_table=table) }}

  {% else %}

    {{ return(materialization_test_default()) }}

  {% endif %}

  {{ return({'relations': relations}) }}
{% endmaterialization %}
