{% materialization table, adapter='polars', supported_languages=['sql', 'python'] %}

  {%- set language = model['language'] -%}

  {% call statement('main', language=language) -%}
    {%- if language == 'python' -%}
      {{ model['compiled_code'] }}
    {%- else -%}
      {{ get_create_table_as_sql(False, this, sql) }}
    {%- endif -%}
  {%- endcall %}

  {{ return({'relations': [this]}) }}

{% endmaterialization %}
