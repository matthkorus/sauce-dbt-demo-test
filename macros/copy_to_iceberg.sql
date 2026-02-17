-- Use this macro in a post run hook on individual models to copy data to an Iceberg table via a catalog-linked database.

{% macro copy_to_iceberg(source_relation, target_database, incremental_key=none, is_incremental=false) %}
    {% set catalog_linked_db = "GLUE_ICEBERG_REST_LINKED_DB" %}
    {% set table_name = source_relation.identifier %}
    {% set columns = adapter.get_columns_in_relation(source_relation) %}

    -- What we'll create/update in Iceberg
    {% set target_relation = '"' ~ catalog_linked_db ~ '"."' ~ target_database ~ '"."' ~ table_name ~ '"' %}
    
    -- Query to check if target table exists
    {% set table_exists_query %}

        SELECT COUNT(*) 
        FROM "{{ catalog_linked_db }}".information_schema.tables 
        WHERE table_schema = '{{ target_database }}' 
        AND table_name = '{{ table_name }}'

    {% endset %}
    
    -- If we haven't built these resouces yet, the following will only run correctly during the build phase.
    -- Check execute to avoid hitting this logic on parsing: 
    {% if execute %}

        {% set results = run_query(table_exists_query) %}
        {% set table_exists = results.columns[0].values()[0] > 0 %}

        {% if not table_exists or not is_incremental %}
            -- Full refresh or initial load of incremental logic

            CREATE OR REPLACE ICEBERG TABLE {{ target_relation }} (
                {% for column in columns %}
                -- always wrap in double quotes and lower case or we'll hit issues with Glue Catalog
                "{{ column.name | lower }}" {% if 'varying' in column.data_type%}VARCHAR(134217728){% else %}{{ column.data_type }}{% endif %}{% if not loop.last %},{% endif %}
                {% endfor %}
            );
            INSERT INTO {{ target_relation }}
            SELECT * FROM {{ source_relation }};
            
            {{ log("Creating Iceberg table: " ~ target_relation, info=true) }}
        
        {% else %}
            -- Incremental load logic

            {% if not incremental_key %}
                {{ exceptions.raise_compiler_error("incremental_key required for incremental loads") }}
            {% endif %}
        
            INSERT INTO {{ target_relation }}
            SELECT * FROM {{ source_relation }}
            WHERE {{ incremental_key }} > (
                SELECT COALESCE(MAX({{ incremental_key }}), '1970-01-01') 
                FROM {{ target_relation }}
            );
            
            {{ log("Incrementally updating Iceberg table: " ~ target_relation, info=true) }}
        
        {% endif %}

    {%endif %}

{% endmacro %}
{% endmacro %}
