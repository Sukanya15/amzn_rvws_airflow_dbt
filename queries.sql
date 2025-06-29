SELECT count(*)
FROM raw_data.stg_xkcd_comics
LIMIT 1000; 

-- Average review rating per category per month:
SELECT
    DD.Year,
    DD.Month,
    DP.Category,
    AVG(FR.Rating) AS AverageRating
FROM
    FactReview FR
JOIN
    DimDate DD ON FR.DatePK = DD.DatePK
JOIN
    DimProduct DP ON FR.ProductPK = DP.ProductPK
GROUP BY
    DD.Year,
    DD.Month,
    DP.Category
ORDER BY
    DD.Year,
    DD.Month,
    DP.Category;

-- Analysis of review rating per brand per month
SELECT
    DD.Year,
    DD.Month,
    DP.Brand,
    AVG(FR.Rating) AS AverageRating,
    COUNT(FR.ReviewPK) AS NumberOfReviews, -- Example of other analysis metric
    MIN(FR.Rating) AS MinimumRating,
    MAX(FR.Rating) AS MaximumRating
FROM
    FactReview FR
JOIN
    DimDate DD ON FR.DatePK = DD.DatePK
JOIN
    DimProduct DP ON FR.ProductPK = DP.ProductPK
GROUP BY
    DD.Year,
    DD.Month,
    DP.Brand
ORDER BY
    DD.Year,
    DD.Month,
    DP.Brand;