{{ config(
    materialized='incremental',
    unique_key='id'
) }}

select *
from {{ source('demo','ZENDESK_USERS') }}

{% if is_incremental() %}
where updated_at > (select max(updated_at) from {{ this }})
{% endif %}