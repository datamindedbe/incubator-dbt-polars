{% macro polars__generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = adapter.get_default_schema(node) -%}

    {%- if custom_schema_name is none -%}

        {{ default_schema }}

    {%- else -%}

        {{ default_schema }}_{{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}
