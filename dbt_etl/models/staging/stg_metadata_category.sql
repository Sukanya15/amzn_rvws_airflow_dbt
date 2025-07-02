{{ config(
    materialized='table'
) }}

SELECT
    product_id,
    image_url,
    sales_category,
    sales_rank,
    first_category,
    product_title,
    product_description,
    product_price,
    related_products,
    product_brand,
    ingestion_timestamp
FROM
    public.processed_metadata_category