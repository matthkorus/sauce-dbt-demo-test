{{ config(materialized='ephemeral') }}
select * from {{ source('MK_DEMO', 'DBTEXAMPLES___ORDERS') }} 