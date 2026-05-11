{{ config(
    materialized='incremental',
    unique_key='id',
    schema='zendesk'
) }}

{% set relation = adapter.get_relation(this.database, this.schema, this.table) %}
{{ log("type=" ~ relation.type ~ ", database=" ~ relation.database ~ ", schema=" ~ relation.schema ~ ", identifier=" ~ relation.identifier, info=true) }}

select *
from {{ source('demo','ZENDESK_USERS') }}
{% if is_incremental() %}
where updated_at > (select max(updated_at) from {{ this }})
{% endif %}