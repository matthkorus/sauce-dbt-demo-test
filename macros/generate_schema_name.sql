{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {% if modules.re.match('^ETLEAP_DBT_PR_\w+$', target.schema) %}
        {%- set default_schema = 'dbt_test' -%}
    {% endif %}

    {%- if custom_schema_name is none -%}

        {{ default_schema }}

    {%- else -%}

        {{ default_schema }}_{{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}