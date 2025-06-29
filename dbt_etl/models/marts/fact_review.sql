-- models/marts/fact_review.sql
{{ config(
    materialized='incremental',
    unique_key='review_sk',
    on_schema_change='fail'
) }}

WITH stg_reviews AS (
    SELECT
        reviewer_id,
        product_id,
        review_timestamp, -- The timestamp of the actual review event
        rating,
        review_text,      -- Now stored directly in the fact table
        ingestion_timestamp -- For incremental loading of the fact table
    FROM
        {{ ref('processed_reviews_data') }}

    {% if is_incremental() %}
    -- Filter for new or updated records since the last successful incremental run
    WHERE ingestion_timestamp > (SELECT MAX(ingestion_timestamp) FROM {{ this }})
    {% endif %}
),
-- Create a separate sentiment calculation model if review_text is huge or calculation is complex
-- For now, let's assume sentiment can be derived within the fact or pre-calculated in a staging model
-- like stg_review_sentiment.py as discussed previously.
-- For simplicity, if stg_review_sentiment.py is already defined and outputs sentiment:
review_sentiment AS (
    SELECT
        reviewer_id,
        product_id,
        review_timestamp,
        sentiment
    FROM
        {{ ref('stg_review_sentiment') }} -- Assuming you create this separate Python model for sentiment
),

-- Reference the SCD Type 2 dimension snapshots
dim_reviewer_scd2 AS (
    SELECT
        reviewer_id,
        dbt_scd_id AS reviewer_sk, -- This is the surrogate key for the reviewer dimension
        dbt_valid_from,
        dbt_valid_to
    FROM
        {{ ref('dim_reviewer_scd2') }} -- Reference the reviewer snapshot table
),
dim_product_scd2 AS (
    SELECT
        product_id,
        dbt_scd_id AS product_sk, -- This is the surrogate key for the product dimension
        dbt_valid_from,
        dbt_valid_to
    FROM
        {{ ref('dim_product_scd2') }} -- Reference the product snapshot table
),
dim_date AS (
    SELECT
        date_sk,
        date AS full_date
    FROM
        {{ ref('dim_date') }}
)
SELECT
    -- Generate a unique surrogate key for each fact record
    {{ dbt_utils.generate_surrogate_key(['sr.reviewer_id', 'sr.product_id', 'sr.review_timestamp']) }} AS review_sk,

    -- Join to get the correct SCD2 surrogate keys for the reviewer and product AT THE TIME OF THE REVIEW
    dr.reviewer_sk,
    dp.product_sk,
    dd.date_sk,

    sr.rating,
    sr.review_text, -- Fact table now holds the review text
    rs.sentiment,   -- Fact table now holds the sentiment (joined from sentiment model)
    sr.ingestion_timestamp

FROM
    stg_reviews sr
LEFT JOIN
    review_sentiment rs
    ON sr.reviewer_id = rs.reviewer_id
    AND sr.product_id = rs.product_id
    AND sr.review_timestamp = rs.review_timestamp
LEFT JOIN
    dim_reviewer_scd2 dr
    ON sr.reviewer_id = dr.reviewer_id
    AND sr.review_timestamp >= dr.dbt_valid_from
    AND (sr.review_timestamp < dr.dbt_valid_to OR dr.dbt_valid_to IS NULL) -- Crucial for SCD2 join
LEFT JOIN
    dim_product_scd2 dp
    ON sr.product_id = dp.product_id
    AND sr.review_timestamp >= dp.dbt_valid_from
    AND (sr.review_timestamp < dp.dbt_valid_to OR dp.dbt_valid_to IS NULL) -- Crucial for SCD2 join
LEFT JOIN
    dim_date dd ON sr.review_timestamp::DATE = dd.full_date

WHERE
    dr.reviewer_sk IS NOT NULL -- Only load facts if a matching SCD2 reviewer record was found
    AND dp.product_sk IS NOT NULL -- Only load facts if a matching SCD2 product record was found
    AND dd.date_sk IS NOT NULL -- Only load facts if a matching date record was found
    AND sr.rating IS NOT NULL
    AND sr.rating >= 1.0 AND sr.rating <= 5.0