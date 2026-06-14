{% macro polars__load_csv_rows(model, agate_table) %}
  {%- set column_types = model['config'].get('column_types', {}) -%}
  {%- do adapter.polars_load_csv_rows(this, agate_table, column_types) -%}
  {{ return('') }}
{% endmacro %}