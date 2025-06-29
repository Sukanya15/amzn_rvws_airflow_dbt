-- models/marts/dim_date.sql
-- dbt_date package generates this table
{{ dbt_date.get_date_dimension('2000-01-01', '2030-12-31') }}

-- Customizations if needed, but dbt_date is usually sufficient.
-- The get_date_dimension macro typically generates columns like:
-- date_day, day_of_week, day_of_month, day_of_year, week_of_year,
-- month, month_name, quarter, year, etc.
-- You might need to rename or select specific columns to match your exact dim_date schema (date_sk, date, day_of_month, month, month_name, quarter, year)
-- Example:
-- SELECT
--     TO_CHAR(date_day, 'YYYYMMDD')::INTEGER AS date_sk,
--     date_day AS date,
--     day_of_month,
--     month_of_year AS month, -- assuming month_of_year from dbt_date matches your month
--     month_name,
--     quarter_of_year AS quarter,
--     year_number AS year
-- FROM {{ dbt_date.get_date_dimension('2000-01-01', '2030-12-31') }}