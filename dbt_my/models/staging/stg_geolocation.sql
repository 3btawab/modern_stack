with source as (
    select * from {{ source('rawdata_source', 'OLIST_GEOLOCATION_DATASET') }}
),

renamed as (
    select
        geolocation_zip_code_prefix::varchar as geolocation_zip_code_prefix,
        geolocation_lat::float as geolocation_lat,
        geolocation_lng::float as geolocation_lng,
        trim(geolocation_city)::varchar as geolocation_city,
        trim(geolocation_state)::varchar as geolocation_state

    from source
)

select * from renamed
