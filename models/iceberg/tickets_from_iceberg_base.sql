
{{
    config(
        materialized='table',
        catalog_name='snowflake_cld',
        contract={'enforced': true}
    )
}}

select * exclude (brand_id, assignee_id,group_id,requester_id,organization_id,submitter_id)
from {{ source('demo','ZENDESK_TICKETS') }}