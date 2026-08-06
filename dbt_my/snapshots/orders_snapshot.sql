{% snapshot orders_snapshot %}

{{
    config(
        unique_key='order_id',
        strategy='check',
        check_cols=['order_status', 'order_delivered_carrier_date', 'order_delivered_customer_date']
    )
}}

select * from {{ ref('stg_orders') }}

{% endsnapshot %}
