{% macro polars__get_empty_subquery_sql(select_sql, select_sql_header=none) %}
  {{ return(select_sql) }}
{% endmacro %}


{% macro polars_assert_contract(sql) %}
  {%- if not config.get('contract').enforced -%}
    {{ return('') }}
  {%- endif -%}

  {%- set constraint_types = [] -%}
  {%- for constraint in model.get('constraints', []) -%}
    {%- do constraint_types.append(constraint['type']) -%}
  {%- endfor -%}
  {%- for column in model['columns'].values() -%}
    {%- for constraint in column.get('constraints', []) -%}
      {%- do constraint_types.append(column['name'] ~ ': ' ~ constraint['type']) -%}
    {%- endfor -%}
  {%- endfor -%}
  {%- if constraint_types -%}
    {{ exceptions.raise_compiler_error(
        "dbt-polars does not support constraints, only column names and data types are enforced. "
        ~ "Remove these constraints: " ~ constraint_types | join(', ')
    ) }}
  {%- endif -%}

  {{ get_assert_columns_equivalent(sql) }}
{% endmacro %}
