{% macro polars__persist_docs(relation, model, for_relation, for_columns) %}
  {% set relation_comment = none %}
  {% if for_relation and config.persist_relation_docs() and model.description %}
    {% set relation_comment = model.description %}
  {% endif %}

  {% set column_comments = {} %}
  {% if for_columns and config.persist_column_docs() and model.columns %}
    {% set existing_columns = adapter.get_columns_in_relation(relation) | map(attribute="name") | list %}
    {% set column_comments = validate_doc_columns(relation, model.columns, existing_columns) or {} %}
  {% endif %}

  {% do adapter.polars_set_comments(relation, relation_comment, column_comments) %}
{% endmacro %}
