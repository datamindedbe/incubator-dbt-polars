{% macro polars__array_construct(inputs, data_type) -%}
    {%- if inputs|length > 0 -%}
[{{ inputs|join(', ') }}]
    {%- else -%}
[]
    {%- endif -%}
{%- endmacro %}
