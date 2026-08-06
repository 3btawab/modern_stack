select
    p.product_category_name,
    count(distinct oi.product_id)                       as product_count,
    count(distinct oi.order_id)                         as order_count,
    sum(oi.price)                                       as total_sales,
    avg(oi.price)                                       as avg_price,
    avg(p.product_weight_g)                             as avg_weight_g,
    avg(p.product_volume_cm3)                           as avg_volume_cm3

from {{ ref('dim_products') }} p
join {{ ref('fct_order_items') }} oi
    on p.product_id = oi.product_id
group by 1
order by total_sales desc
