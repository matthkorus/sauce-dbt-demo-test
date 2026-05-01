/*
{{
    config(
        materialized='table',
        catalog_name='snowflake_cld'
    )
}}

SELECT 'etleap_iceberg_base' AS source, COUNT(*) AS cnt FROM {{ref('tickets_from_iceberg_base')}}
UNION ALL
SELECT 'catalog_linked', COUNT(*) FROM {{ref('tickets_from_cld')}};
*/