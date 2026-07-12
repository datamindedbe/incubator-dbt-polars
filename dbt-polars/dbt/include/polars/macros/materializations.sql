{% materialization view, adapter='polars' %}
  {{ exceptions.raise_compiler_error("The polars adapter does not support view materializations.") }}
{% endmaterialization %}


{% materialization incremental, adapter='polars', supported_languages=['sql', 'python'] %}
  {%- set existing_relation = load_relation(this) -%}
  {%- set target_relation = this.incorporate(type='table') -%}
  {%- set full_refresh_mode = (should_full_refresh()) -%}
  {%- set language = model['language'] -%}
  {%- set partition_by = config.get('partition_by') -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% if language == 'python' %}
    {% if existing_relation is not none and full_refresh_mode %}
      {%- do adapter.drop_relation(existing_relation) -%}
    {% endif %}
    {% call statement('main', language='python') -%}
      {{ compiled_code }}
    {%- endcall %}
  {% else %}
    {% if existing_relation is none %}
      {%- do adapter.polars_execute_model(target_relation, sql, model['extra_ctes'], partition_by, model.get('config', {})) -%}
    {% elif full_refresh_mode %}
      {%- do adapter.drop_relation(existing_relation) -%}
      {%- do adapter.polars_execute_model(target_relation, sql, model['extra_ctes'], partition_by, model.get('config', {})) -%}
    {% else %}
      {%- set unique_key = config.get('unique_key') -%}
      {%- set strategy = config.get('incremental_strategy') or (unique_key and 'merge') or 'append' -%}
      {%- set on_schema_change = config.get('on_schema_change') or 'ignore' -%}
      {%- set merge_update_columns = config.get('merge_update_columns') -%}
      {%- set merge_exclude_columns = config.get('merge_exclude_columns') -%}
      {%- set incremental_predicates = config.get('predicates') or config.get('incremental_predicates') -%}
      {%- do adapter.polars_execute_incremental_model(target_relation, sql, unique_key, strategy, on_schema_change, merge_update_columns, merge_exclude_columns, incremental_predicates, model['extra_ctes'], partition_by, model.get('config', {})) -%}
    {% endif %}
    {% call statement('main') -%}
      {{ sql }}
    {%- endcall %}
  {% endif %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}
  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {% do persist_docs(target_relation, model) %}

  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}


{% materialization table, adapter='polars', supported_languages=['sql', 'python'] %}
  {%- set target_relation = this.incorporate(type='table') -%}
  {%- set language = model['language'] -%}
  {%- set partition_by = config.get('partition_by') -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% if language == 'python' %}
    {% call statement('main', language='python') -%}
      {{ compiled_code }}
    {%- endcall %}
  {% else %}
    {%- do adapter.polars_execute_model(target_relation, sql, model['extra_ctes'], partition_by, model.get('config', {})) -%}
    {% call statement('main') -%}
      {{ sql }}
    {%- endcall %}
  {% endif %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}
  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {% do persist_docs(target_relation, model) %}

  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}

{# TODO: review whether dbt clone can be supported for the polars adapter #}
{% materialization clone, adapter='polars' %}
  {{ exceptions.raise_compiler_error("The polars adapter does not support dbt clone.") }}
{% endmaterialization %}


{% materialization snapshot, adapter='polars' %}
  {%- set target_relation = this.incorporate(type='table') -%}

  {%- do adapter.polars_execute_snapshot(
        target_relation,
        sql,
        config.get('unique_key'),
        config.get('strategy'),
        config.get('updated_at'),
        config.get('check_cols'),
        adapter.get_hard_deletes_behavior(config),
        model['extra_ctes']
  ) -%}

  {% call statement('main') -%}
    -- Source query of the snapshot model. The SCD2 merge is handled by the Polars adapter in Python.
    {{ sql }}
  {%- endcall %}

  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
