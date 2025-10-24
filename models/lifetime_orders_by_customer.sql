with order_items as ( 
    select 
        customer_name, 
        item_quantity,
        item_unit_cost
    from {{ref('order_info')}} oi
)
select 
    customer_name, 
    sum(item_quantity*item_unit_cost) as lifetime_spend
from order_items 
group by customer_name
-- test