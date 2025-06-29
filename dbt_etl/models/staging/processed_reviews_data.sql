SELECT
    reviewer_id,
    product_id,
    reviewer_name,
    helpful_votes, -- Consider casting if this needs to be numeric
    review_text,
    rating,
    review_summary,
    unix_review_timestamp,
    review_timestamp, -- Use this for date dimension, assuming it's reliable
    ingestion_timestamp
FROM
    public.processed_reviews_data -- Reference your raw table