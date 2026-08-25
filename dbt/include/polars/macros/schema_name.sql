{% macro polars__generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target_schema(node=node) -%}

    {%- if custom_schema_name is none -%}

        {{ default_schema }}

    {%- else -%}

        {{ default_schema }}_{{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}

{% macro target_schema(catalog=none, node=none) -%}
    {%- if node is not none -%}
        {%- set catalog = (node.config.get("catalog") if node.config else none) or node.database -%}
    {%- endif -%}
    {{ return(adapter.get_default_schema({"database": catalog})) }}
{%- endmacro %}
