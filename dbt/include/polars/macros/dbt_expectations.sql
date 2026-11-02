{# Used by dbt_expectations' regex tests. Needs dispatch config, see docs/dbt-compatibility.md #}
{% macro polars__regexp_instr(source_value, regexp, position, occurrence, is_raw, flags) %}
{% if flags %}{{ dbt_expectations._validate_flags(flags, 'imsx') }}{% endif %}
regexp_like({{ source_value }}, '{{ regexp }}'{% if flags %}, '{{ flags }}'{% endif %})
{% endmacro %}
