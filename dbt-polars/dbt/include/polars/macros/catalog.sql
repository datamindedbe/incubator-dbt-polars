{% macro polars__persist_docs(relation, model, for_relation, for_columns) %}
  {% if for_relation and config.persist_relation_docs() and model.description %}
    {% do adapter.polars_set_relation_comment(relation, model.description) %}
  {% endif %}

  {% if for_columns and config.persist_column_docs() and model.columns %}
    {% set existing_columns = adapter.get_columns_in_relation(relation) | map(attribute="name") | list %}
    {% set filtered_columns = validate_doc_columns(relation, model.columns, existing_columns) %}
    {% if filtered_columns %}
      {% do adapter.polars_set_column_comments(relation, filtered_columns) %}
    {% endif %}
  {% endif %}
{% endmacro %}