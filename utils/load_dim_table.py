# # -- DimDate Table
# CREATE TABLE DimDate (
#     DatePK INT PRIMARY KEY,         -- YYYYMMDD
#     FullDate DATE NOT NULL,
#     DayOfMonth TINYINT NOT NULL,
#     Month TINYINT NOT NULL,
#     Quarter TINYINT NOT NULL,
#     Year SMALLINT NOT NULL,
#     DayOfWeek TINYINT NOT NULL,
#     WeekOfYear TINYINT NOT NULL,
#     MonthName VARCHAR(20) NOT NULL
# );

# # -- DimProduct Table
# CREATE TABLE DimProduct (
#     ProductPK INT IDENTITY(1,1) PRIMARY KEY, -- Surrogate Key
#     ProductID VARCHAR(20) UNIQUE NOT NULL,  -- Natural Key (asin)
#     ProductName VARCHAR(255),
#     Brand VARCHAR(100),
#     Category VARCHAR(100)
# );

# # -- DimReviewer Table
# CREATE TABLE DimReviewer (
#     ReviewerPK INT IDENTITY(1,1) PRIMARY KEY, -- Surrogate Key
#     ReviewerID VARCHAR(50) UNIQUE NOT NULL,   -- Natural Key
#     ReviewerName VARCHAR(255),
#     ReviewerLocation VARCHAR(255),            -- Optional
#     ReviewerEmail VARCHAR(255)                -- Optional (handle PII)
# );
