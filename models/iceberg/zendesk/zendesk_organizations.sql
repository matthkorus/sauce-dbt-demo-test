{{ config(
    materialized='table',
    unique_key='id'
) }}

select *
from {{ source('demo','ORGANIZATIONS') }}
