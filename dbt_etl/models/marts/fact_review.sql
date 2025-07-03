{{ config(
    materialized='incremental',
    unique_key='review_sk',
    on_schema_change='fail',
    incremental_strategy='delete+insert'
) }}

SELECT
    {{ dbt_utils.generate_surrogate_key([
        'reviews.reviewer_id',
        'reviews.product_id',
        'reviews.unix_review_timestamp'
    ]) }} AS review_sk,

    reviews.reviewer_id::TEXT AS reviewer_id,
    reviews.product_id::TEXT AS product_id,
    dates.date_day AS date_sk,

    reviews.review_summary::TEXT AS review_summary,
    reviews.rating::FLOAT AS rating,
    reviews.sentiment::TEXT AS sentiment,
    reviews.ingestion_timestamp

FROM {{ ref('stg_reviews_data') }} reviews 

LEFT JOIN {{ ref('dim_product_scd2') }} products_scd
    ON reviews.product_id::TEXT = products_scd.product_id::TEXT
    AND reviews.review_timestamp BETWEEN products_scd.dbt_valid_from AND COALESCE(products_scd.dbt_valid_to, '9999-12-31'::TIMESTAMP)

LEFT JOIN {{ ref('dim_reviewer_scd2') }} reviewers_scd
    ON reviews.reviewer_id::TEXT = reviewers_scd.reviewer_id::TEXT
    AND reviews.review_timestamp BETWEEN reviewers_scd.dbt_valid_from AND COALESCE(reviewers_scd.dbt_valid_to, '9999-12-31'::TIMESTAMP)

LEFT JOIN {{ ref('dim_date') }} dates
    ON reviews.review_timestamp::DATE = dates.date_day

{% if is_incremental() %}
    WHERE reviews.ingestion_timestamp > (SELECT MAX(ingestion_timestamp) FROM {{ this }})
{% endif %}