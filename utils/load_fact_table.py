
# # -- FactReview Table
# CREATE TABLE FactReview (
#     ReviewPK BIGINT IDENTITY(1,1) PRIMARY KEY, -- Surrogate Key for each review
#     DatePK INT NOT NULL,
#     ProductPK INT NOT NULL,
#     ReviewerPK INT NOT NULL,
#     Rating TINYINT NOT NULL,                  -- 1-5
#     VoteCount INT,                            -- Total votes
#     HelpfulVotes INT,                         -- Helpful votes
#     ReviewText TEXT,                          -- Optional, if analysis on text is needed
#     FOREIGN KEY (DatePK) REFERENCES DimDate(DatePK),
#     FOREIGN KEY (ProductPK) REFERENCES DimProduct(ProductPK),
#     FOREIGN KEY (ReviewerPK) REFERENCES DimReviewer(ReviewerPK)
# );