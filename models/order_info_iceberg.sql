{{
    config(
        materialized='table',
        table_format = 'iceberg',
        external_volume = 'MK_ICEBERG_EXTERNAL_VOLUME'
    )
}}

select * from {{ ref('order_info') }}