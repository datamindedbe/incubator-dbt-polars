{#- Polars' POSITION treats the search text as a regex, so a string literal is regex-escaped
    to search literally. Other expressions can't be escaped in Polars SQL and pass through. -#}
{% macro polars__position(substring_text, string_text) -%}
    {%- set literal = modules.re.fullmatch("'((?:[^']|'')*)'", substring_text | string | trim) -%}
    {%- if literal -%}
        {%- set text = literal.group(1).replace("''", "'") -%}
        {%- set substring_text = "'" ~ modules.re.escape(text).replace("'", "''") ~ "'" -%}
    {%- endif -%}
position({{ substring_text }} in {{ string_text }})
{%- endmacro %}
