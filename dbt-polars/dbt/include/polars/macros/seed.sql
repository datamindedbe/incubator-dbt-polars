{% macro polars__load_csv_rows(model, agate_table) %}
  {%- set column_types = model['config'].get('column_types', {}) -%}
  {%- set partition_by = model['config'].get('partition_by') -%}
  {%- do adapter.polars_load_csv_rows(this, agate_table, column_types, partition_by) -%}
  {{ return('') }}
{% endmacro %}
