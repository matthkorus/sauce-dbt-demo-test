select 
	t.id as ticket_id, 
    t.subject as ticket_subject,
    t.status,
    r.email as requester_email,
    r.name as requester_name,
    a.email as assignee_email,
    a.name as assignee_name,
    o.name as organization_name
from {{ ref('zendesk_tickets') }} t
inner join {{ ref('zendesk_users') }} r on t.requester_id = r.id 
inner join {{ ref('zendesk_users') }} a on t.assignee_id = a.id
inner join {{ ref('zendesk_organizations') }} o on r.organization_id = o.id

{% if is_incremental() %}
where t.updated_at > (select max(t.updated_at) from {{ this }})
{% endif %}