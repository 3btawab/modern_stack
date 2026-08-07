with source as (
    select * from {{ source('rawdata_source', 'OLIST_ORDER_ITEMS_DATASET') }}
),

renamed as (
    select
        order_id::varchar as order_id,
        order_item_id::integer as order_item_id,
        product_id::varchar as product_id,
        seller_id::varchar as seller_id,
        shipping_limit_date,
        price::decimal(10, 2) as price,
        freight_value::decimal(10, 2) as freight_value

    from source
)

select * from renamed
