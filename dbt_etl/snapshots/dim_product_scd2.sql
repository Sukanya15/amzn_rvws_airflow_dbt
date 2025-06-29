-- snapshots/dim_product_scd2.sql
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
    -- Apply your category logic consistently here as well
    COALESCE(first_category, sales_category, 'Uncategorized') AS category,
    product_brand AS brand,
    product_price -- This attribute is included but not "checked" for SCD2 changes
FROM
    {{ ref('processed_metadata_category') }}
WHERE
    product_id IS NOT NULL -- Ensure we only track valid product IDs

{% endsnapshot %}