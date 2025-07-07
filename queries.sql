-- Average review rating per category per month:
SELECT
    DD.year_number,
    DD.month_name,
    DP.category,
    AVG(FR.rating) AS avg_rating
FROM
    fact_review FR
JOIN
    dim_date DD ON FR.date_sk = DD.date_day
JOIN
    dim_product_scd2 DP ON FR.product_id = DP.product_id
GROUP BY
    DD.year_number,
    DD.month_name,
    DP.category
ORDER BY
    DD.year_number,
    DD.month_name,
    DP.category;

-- Analysis of review rating per brand per month
SELECT
    DD.year_number,
    DD.month_name,
    DP.brand,
    AVG(FR.rating) AS avg_rating
FROM
    fact_review FR
JOIN
    dim_date DD ON FR.date_sk = DD.date_day
JOIN
    dim_product_scd2 DP ON FR.product_id = DP.product_id
WHERE
	brand IS NOT NULL
GROUP BY
    DD.year_number,
    DD.month_name,
    DP.brand
ORDER BY
    DD.year_number,
    DD.month_name,
    DP.brand;

-- Total count for each brand with its sentiment count
SELECT DP.brand,
    SUM(CASE WHEN FR.sentiment = 'Positive' THEN 1 ELSE 0 END) AS positive_count,
	SUM(CASE WHEN FR.sentiment = 'Negative' THEN 1 ELSE 0 END) AS negative_count
FROM fact_review FR
JOIN dim_date DD ON FR.date_sk = DD.date_day
JOIN dim_product_scd2 DP ON FR.product_id = DP.product_id
WHERE DP.brand IS NOT NULL
	-- DP.brand = 'Sloggers'
GROUP BY DP.brand
ORDER BY DP.brand;


-- Brand with maximum positive and negative sentiment counts
WITH temp_table AS (SELECT
    DP.brand,
    SUM(CASE WHEN FR.sentiment = 'Positive' THEN 1 ELSE 0 END) AS positive_count,
	SUM(CASE WHEN FR.sentiment = 'Negative' THEN 1 ELSE 0 END) AS negative_count
FROM fact_review FR
JOIN dim_date DD ON FR.date_sk = DD.date_day
JOIN dim_product_scd2 DP ON FR.product_id = DP.product_id
WHERE DP.brand IS NOT NULL
GROUP BY DP.brand
ORDER BY DP.brand),

ranked_brands AS (SELECT
        brand,
        positive_count,
        negative_count,
        ROW_NUMBER() OVER (ORDER BY positive_count DESC) AS rn_positive,
        ROW_NUMBER() OVER (ORDER BY negative_count DESC) AS rn_negative
    FROM temp_table)
	
SELECT brand, positive_count AS count_value, 'Max Positive' AS count_type
FROM ranked_brands
WHERE rn_positive = 1

UNION ALL

SELECT brand, negative_count AS count_value, 'Max Negative' AS count_type
FROM ranked_brands
WHERE rn_negative = 1;