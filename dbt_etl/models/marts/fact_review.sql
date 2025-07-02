{{ config(
    materialized='incremental',
    unique_key='review_sk',
    on_schema_change='fail'
) }}

SELECT
    {{ dbt_utils.generate_surrogate_key([
        'pr.reviewer_id',
        'dp.product_id',
        'pr.unix_review_timestamp'
    ]) }} AS review_sk,

    pr.reviewer_id::TEXT AS reviewer_id,
    dp.product_id::TEXT AS product_id,
    TO_CHAR(pr.review_timestamp, 'YYYYMMDD')::INTEGER AS date_sk,

    pr.rating::FLOAT AS rating,
    pr.sentiment::TEXT AS sentiment

FROM {{ source('public_data', 'stg_reviews_data') }} pr

LEFT JOIN {{ ref('dim_product') }} dp
    ON pr.product_id::TEXT = dp.product_id::TEXT

{% if is_incremental() %}
    WHERE pr.ingestion_timestamp > (SELECT MAX(ingestion_timestamp) FROM {{ this }})
{% endif %}