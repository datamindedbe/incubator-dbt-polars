{% macro polars_raise_if_contract_enforced() %}
  {%- if config.get('contract').enforced -%}
    {{ exceptions.raise_compiler_error(
        "Model contracts are not yet supported in dbt-polars: remove 'contract: {enforced: true}' from "
        ~ model['name'] ~ "."
    ) }}
  {%- endif -%}
{% endmacro %}
