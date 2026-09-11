{#
    By default dbt glues the target schema in front of a custom schema:
    +schema: marts would become "staging_marts". This macro uses the custom
    schema name exactly as written, so models land in staging and marts.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}