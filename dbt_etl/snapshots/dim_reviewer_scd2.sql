{% snapshot dim_reviewer_scd2 %}

{{
    config(
        target_schema='public',
        unique_key='reviewer_id',
        strategy='check',
        check_cols=['reviewer_name'],
        invalidate_hard_deletes=True 
    )
}}

SELECT
    reviewer_id,
    reviewer_name
FROM
    {{ ref('stg_reviews_data') }}
WHERE
    reviewer_id IS NOT NULL

{% endsnapshot %}