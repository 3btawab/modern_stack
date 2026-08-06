with source as (
    select * from {{ source('rawdata_source', 'OLIST_SELLERS_DATASET') }}
),

renamed as (
    select
        seller_id::varchar                                                 as seller_id,
        seller_zip_code_prefix::varchar                                    as seller_zip_code_prefix,
        trim(seller_city)::varchar                                         as seller_city,
        trim(seller_state)::varchar                                        as seller_state

    from source
)

select * from renamed
