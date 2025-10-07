with orders as ( 
    select * 
    from {{ source('mk_test', 'orders') }}
), 
order_items as ( 
    select * 
    from {{ source('mk_test', 'order_items') }}
),
skus as ( 
    select * 
    from {{ source('mk_test', 'skus') }}
),
user_names as ( 
    select *
    from {{ source('mk_test', 'user_names') }}
    where id > 1
)
select 
    o.id as order_id,
    o.order_date, 
    u.first_name || ' ' || u.last_name as customer_name,
    u.email as customer_email,
    s.sku as sku_sku,
    s.name as item_name, 
    s.description as item_description,
    oi.quantity as item_quantity,
    s.cost as item_unit_cost
from orders o
left join order_items oi
    on oi.order_id_ = o.id 
left join skus s 
    on oi.sku = s.sku
left join user_names u 
    on o.user_id = u.id

    