{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    {%- set is_etleap_ci = modules.re.match('^ETLEAP_DBT_PR_\w+$', target.schema) -%} 

    {%- if is_etleap_ci -%}

        {%- set default_schema = target.schema | lower -%}

        {%- if custom_schema_name is none -%}

            {{ default_schema }}

        {%- else -%}

            {{ default_schema }}_{{ custom_schema_name | trim }}
        
        {%- endif -%}

    {%- else -%}
    
        {%- if custom_schema_name is none -%}

            {{ default_schema }}

        {%- else -%}

            {{ custom_schema_name }}
        
        {%- endif -%}

    {%- endif -%}

{%- endmacro %}