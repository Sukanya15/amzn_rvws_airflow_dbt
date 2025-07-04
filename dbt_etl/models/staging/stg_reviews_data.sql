{{ config(
    materialized='table'
) }}

SELECT
    reviewer_id,
    product_id,
    reviewer_name,
    rating,
    review_summary,
    COALESCE(sentiment, 'None') AS sentiment,
    unix_review_timestamp,
    review_timestamp,
    ingestion_timestamp
FROM
    public.processed_reviews_data