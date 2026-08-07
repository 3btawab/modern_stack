with source as (
    select * from {{ source('rawdata_source', 'OLIST_ORDERS_DATASET') }}
),

renamed as (
    select
        order_id::varchar as order_id,
        customer_id::varchar as customer_id,
        lower(trim(order_status))::varchar as order_status,
        order_purchase_timestamp,
        order_approved_at::timestamp_ntz as order_approved_at,
        order_delivered_carrier_date,
        order_delivered_customer_date,
        order_estimated_delivery_date

    from source
)

select * from renamed
