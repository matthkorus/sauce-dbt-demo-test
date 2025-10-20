select 
	t.id as ticket_id, 
    t.subject as ticket_subject,
    tc.body as comment_body, 
    tc.created_at as comment_created_at,
    tc.type,
    tc.public,
    u.email as requester_email,
    u.name as requester_name,
    o.name as organization_name
from {{ source('zendesk', 'Tickets') }} t
left join {{ source('zendesk', 'ticket_comments') }} tc on tc.ticket_id = t.id
inner join {{ source('zendesk', 'Users') }}u on t.requester_id = u.id 
inner join {{ source('zendesk', 'Organizations') }} o on u.organization_id = o.id