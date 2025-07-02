{{ config(
    materialized='table',
    unique_key='reviewer_id'
) }}

SELECT
    reviewer_id::TEXT AS reviewer_id,
    reviewer_name::TEXT AS reviewer_name,
    review_summary::TEXT AS review_summary,
    review_date::TEXT AS review_date
FROM {{ source('public_data', 'stg_reviews_data') }}
WHERE reviewer_id IS NOT NULL
GROUP BY
    reviewer_id,
    reviewer_name,
    review_summary,
    review_date