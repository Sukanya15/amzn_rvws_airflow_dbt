CREATE DATABASE amazon_reviews_dwh;
CREATE DATABASE metabase_app_db;

GRANT ALL PRIVILEGES ON DATABASE amazon_reviews_dwh TO airflow;
GRANT ALL PRIVILEGES ON DATABASE metabase_app_db TO airflow;