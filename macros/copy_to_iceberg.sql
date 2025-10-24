{% macro copy_to_iceberg(source_relation, target_database) %}
    {% set catalog_linked_db = "GLUE_ICEBERG_REST_LINKED_DB" %}

    {% set table_name = source_relation.identifier %}
    {% set columns = adapter.get_columns_in_relation(source_relation) %}
    
    CREATE OR REPLACE ICEBERG TABLE "{{ catalog_linked_db }}"."{{ target_database }}"."{{ table_name }}" (
        {% for column in columns %}
         -- Varchar length differs between snowflake and iceberg, so let's sanitize
        "{{ column.name }}" {% if 'varying' in column.data_type%}VARCHAR(134217728){% else %}{{ column.data_type }}{% endif %}{% if not loop.last %},{% endif %}
        {% endfor %}
    );

    INSERT INTO "{{ catalog_linked_db }}"."{{ target_database }}"."{{ table_name }}"
    SELECT * FROM {{ source_relation }};
{% endmacro %}