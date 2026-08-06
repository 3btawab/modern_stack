with source as (
    select * from {{ source('rawdata_source', 'OLIST_ORDER_PAYMENTS_DATASET') }}
),

renamed as (
    select
        order_id::varchar                                                  as order_id,
        payment_sequential::integer                                        as payment_sequential,
        lower(trim(payment_type))::varchar                                 as payment_type,
        payment_installments::integer                                      as payment_installments,
        payment_value::decimal(10, 2)                                      as payment_value

    from source
)

select * from renamed
