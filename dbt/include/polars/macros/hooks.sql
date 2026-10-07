{% macro run_hooks(hooks, inside_transaction=True) %}
  {% for hook in hooks | selectattr('transaction', 'equalto', inside_transaction) %}
    {% set rendered = render(hook.get('sql')) | trim %}
    {% if (rendered | length) > 0 %}
      {{ exceptions.raise_compiler_error(
          "dbt-polars does not support SQL hooks: there is no database to run them on. "
          ~ "Hooks that only call macros are supported. Hook SQL:\n" ~ rendered
      ) }}
    {% endif %}
  {% endfor %}
{% endmacro %}


{% macro polars__apply_grants(relation, grant_config, should_revoke=True) %}
  {% if grant_config %}
    {{ exceptions.raise_compiler_error(
        "dbt-polars does not support grants: remove the 'grants' config from " ~ relation.render() ~ "."
    ) }}
  {% endif %}
{% endmacro %}
