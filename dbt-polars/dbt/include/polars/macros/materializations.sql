{% materialization view, adapter='polars' %}
  {{ exceptions.raise_compiler_error("The polars adapter does not support view materializations.") }}
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
