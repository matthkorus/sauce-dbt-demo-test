
select * exclude (audit_id, author_id, id, ticket_id)
from {{ source('demo','TICKET_COMMENTS') }} tc
/* 
{% if is_incremental() %}
    where tc.created_at > (select max(created_at) from {{ this }})
{% endif %}
*/