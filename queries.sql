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