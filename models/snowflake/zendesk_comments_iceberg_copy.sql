{{ 
	config(
        materialized='incremental',
	      post_hook="{{copy_to_iceberg(this,'dbt_test',incremental_key='created_at', is_incremental=is_incremental())}}"
    )
}}

select * 
from {{ source('demo', 'TICKET_COMMENTS') }}

{% if is_incremental() %}

where created_at > (select coalesce(max(created_at),'1900-01-01') from {{ this }} )

{% endif %}