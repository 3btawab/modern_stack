select
    s.seller_id,
    s.seller_city,
    s.seller_state,
    count(distinct oi.order_id)                         as total_orders,
    count(distinct oi.product_id)                       as unique_products_sold,
    sum(oi.price)                                       as total_sales,
    sum(oi.freight_value)                               as total_freight,
    avg(oi.price)                                       as avg_item_price,
    sum(oi.price + oi.freight_value)                    as total_revenue,
    total_revenue / count(distinct oi.order_id)         as avg_order_value

from {{ ref('dim_sellers') }} s
join {{ ref('fct_order_items') }} oi
    on s.seller_id = oi.seller_id
where oi.order_status = 'delivered'
group by 1, 2, 3
order by total_revenue desc
limit 100
