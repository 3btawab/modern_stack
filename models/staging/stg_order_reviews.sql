with source as (
    select * from {{ source('rawdata_source', 'OLIST_ORDER_REVIEWS_DATASET') }}
),

renamed as (
    select
        review_id::varchar  as review_id,
        order_id::varchar   as order_id,
        review_score::integer  as review_score,
        review_comment_title::varchar as review_comment_title,
        review_comment_message::varchar   as review_comment_message,
        review_creation_date,
        review_answer_timestamp

    from source
)

select * from renamed
