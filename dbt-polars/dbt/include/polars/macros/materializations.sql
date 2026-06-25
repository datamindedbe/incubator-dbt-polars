{% materialization view, adapter='polars' %}
  {{ exceptions.raise_compiler_error("The polars adapter does not support view materializations.") }}
{% endmaterialization %}


{% materialization incremental, adapter='polars' %}
  {%- set existing_relation = load_relation(this) -%}
  {%- set target_relation = this.incorporate(type='table') -%}
  {%- set full_refresh_mode = (should_full_refresh()) -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% if existing_relation is none %}
    {%- do adapter.polars_execute_model(target_relation, sql) -%}
  {% elif full_refresh_mode or existing_relation.is_view %}
    {%- do adapter.drop_relation(existing_relation) -%}
    {%- do adapter.polars_execute_model(target_relation, sql) -%}
  {% else %}
    {%- set unique_key = config.get('unique_key') -%}
    {%- set strategy = config.get('incremental_strategy') or (unique_key and 'merge') or 'append' -%}
    {%- set on_schema_change = config.get('on_schema_change') or 'ignore' -%}
    {%- set merge_update_columns = config.get('merge_update_columns') -%}
    {%- set merge_exclude_columns = config.get('merge_exclude_columns') -%}
    {%- set incremental_predicates = config.get('predicates') or config.get('incremental_predicates') -%}
    {%- do adapter.polars_execute_incremental_model(target_relation, sql, unique_key, strategy, on_schema_change, merge_update_columns, merge_exclude_columns, incremental_predicates) -%}
  {% endif %}

  {% call statement('main') -%}
    {{ sql }}
  {%- endcall %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}
  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {% do persist_docs(target_relation, model) %}

  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}


{% materialization table, adapter='polars' %}
  {%- set target_relation = this.incorporate(type='table') -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {%- do adapter.polars_execute_model(target_relation, sql) -%}

  {% call statement('main') -%}
    {{ sql }}
  {%- endcall %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}
  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {% do persist_docs(target_relation, model) %}

  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}

{# TODO: review whether dbt clone can be supported for the polars adapter #}
{% materialization clone, adapter='polars' %}
  {{ exceptions.raise_compiler_error("The polars adapter does not support dbt clone.") }}
{% endmaterialization %}
