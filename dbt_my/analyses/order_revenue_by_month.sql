select
    date_trunc('month', order_purchase_timestamp) as order_month,
    order_status,
    count(order_id)          as order_count,
    sum(total_payment_value) as total_revenue,
    avg(total_payment_value) as avg_order_value,
    avg(delivery_days)       as avg_delivery_days,
    avg(avg_review_score)    as avg_review_score

from {{ ref('fct_orders') }}
where order_status = 'delivered'
group by 1, 2
order by 1, 2
