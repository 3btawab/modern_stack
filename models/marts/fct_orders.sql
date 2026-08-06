with orders as (
    select * from {{ ref('stg_orders') }}
),

payments_agg as (
    select
        order_id,
        sum(payment_value)                       as total_payment_value,
        max(payment_sequential)                  as payment_count,
        count(distinct payment_type)             as distinct_payment_types,
        max(payment_installments)                as max_installments

    from {{ ref('stg_order_payments') }}
    group by 1
),

reviews_agg as (
    select
        order_id,
        avg(review_score)          as avg_review_score,
        count(review_id)           as review_count

    from {{ ref('stg_order_reviews') }}
    group by 1
)

select
    o.order_id,
    o.customer_id,
    o.order_status,
    o.order_purchase_timestamp,
    o.order_approved_at,
    o.order_delivered_carrier_date,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date,

    coalesce(p.total_payment_value, 0) as total_payment_value,
    coalesce(p.payment_count, 0) as payment_count,
    p.distinct_payment_types,
    p.max_installments,

    r.avg_review_score,
    coalesce(r.review_count, 0) as review_count,

    datediff(
        'day', o.order_purchase_timestamp, o.order_delivered_customer_date
    ) as delivery_days,
    datediff(
        'day', o.order_estimated_delivery_date, o.order_delivered_customer_date
    ) as delivery_delay_days

from orders o
left join payments_agg p
    on o.order_id = p.order_id
left join reviews_agg r
    on o.order_id = r.order_id
