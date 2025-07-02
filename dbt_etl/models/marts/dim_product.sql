{{ config(
    materialized='table',
    unique_key='product_id'
) }}

SELECT
    product_id::TEXT AS product_id,
    product_title::TEXT AS product_title,
    brand::TEXT AS brand,
    category::TEXT AS category,
    price::FLOAT AS price 
FROM {{ source('public_data', 'stg_metadata_category') }}
WHERE product_id IS NOT NULL
GROUP BY
    product_id,
    product_title,
    brand,
    category,
    price