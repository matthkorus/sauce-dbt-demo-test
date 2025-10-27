{% macro copy_to_iceberg(source_relation, target_database, incremental_key=none) %}
    {% set catalog_linked_db = "GLUE_ICEBERG_REST_LINKED_DB" %}
    {% set table_name = source_relation.identifier %}
    {% set columns = adapter.get_columns_in_relation(source_relation) %}
    
    {% set target_relation = '"' ~ catalog_linked_db ~ '"."' ~ target_database ~ '"."' ~ table_name ~ '"' %}
    
    {% if not is_incremental() %}
        -- Full refresh
        CREATE OR REPLACE ICEBERG TABLE {{ target_relation }} (
            {% for column in columns %}
            "{{ column.name }}" {% if 'varying' in column.data_type%}VARCHAR(134217728){% else %}{{ column.data_type }}{% endif %}{% if not loop.last %},{% endif %}
            {% endfor %}
        );
        INSERT INTO {{ target_relation }}
        SELECT * FROM {{ source_relation }};
    {% else %}
        -- Incremental load
        {% if not incremental_key %}
            {{ exceptions.raise_compiler_error("incremental_key required for incremental loads") }}
        {% endif %}
        
        INSERT INTO {{ target_relation }}
        SELECT * FROM {{ source_relation }}
        WHERE {{ incremental_key }} > (
            SELECT COALESCE(MAX({{ incremental_key }}), '1970-01-01') 
            FROM {{ target_relation }}
        );
    {% endif %}
{% endmacro %}