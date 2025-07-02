{% snapshot dim_product_scd2 %}

{{
    config(
        target_schema='public',
        unique_key='product_id',
        strategy='check',
        check_cols=['product_title', 'category', 'brand'],
        invalidate_hard_deletes=True
    )
}}

SELECT
    product_id,
    product_title,
    COALESCE(first_category, sales_category, 'Uncategorized') AS category,
    product_brand AS brand,
    product_price
FROM
    {{ source('public_data', 'stg_metadata_category') }}
WHERE
    product_id IS NOT NULL

{% endsnapshot %}