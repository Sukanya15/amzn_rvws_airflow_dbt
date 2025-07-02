import psycopg2
import os
from psycopg2 import sql

DB_HOST = os.getenv('ARDWH_HOST', 'postgres')
DB_NAME = os.getenv('ARDWH_DB', 'amazon_reviews_dwh')
DB_USER = os.getenv('ARDWH_USER', 'airflow')
DB_PASSWORD = os.getenv('ARDWH_PASSWORD', 'airflow')
DB_PORT = os.getenv('ARDWH_PORT', '5432')

def get_db_connection():
    """Establishes and returns a PostgreSQL database connection."""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT
        )
        conn.autocommit = False
        print(f"Attempting to connect to DB at host: {DB_HOST}, port: {DB_PORT}, db: {DB_NAME}")
        print("Successfully connected to the database.")
        return conn
    except Exception as e:
        print(f"Error connecting to the database at host {DB_HOST}: {e}")
        raise

def create_processed_metadata_table(conn, table_name="processed_metadata_category"):
    PROCESSED_TABLE_NAME = table_name

    print(f"Creating table '{PROCESSED_TABLE_NAME}' if it does not exist...")
    create_table_query = sql.SQL("""
        CREATE TABLE IF NOT EXISTS public.{} (
            metadata_id TEXT,
            product_id TEXT PRIMARY KEY,
            image_url TEXT,
            sales_category TEXT,
            sales_rank INTEGER,
            first_category TEXT,
            product_title TEXT,
            product_description TEXT,
            product_price NUMERIC,
            related_products TEXT,
            product_brand TEXT,
            ingestion_timestamp TIMESTAMP WITHOUT TIME ZONE
        );
    """).format(sql.Identifier(PROCESSED_TABLE_NAME))

    with conn.cursor() as cur:
        cur.execute(create_table_query)
    conn.commit()
    print(f"Table '{PROCESSED_TABLE_NAME}' created or already exists.")

def create_processed_reviews_table(conn, table_name="processed_reviews_data"):
    PROCESSED_TABLE_NAME = table_name

    print(f"Creating table '{PROCESSED_TABLE_NAME}' if it does not exist...")
    create_table_query = sql.SQL("""
        CREATE TABLE IF NOT EXISTS public.{} (
            reviewer_id TEXT,
            product_id TEXT,
            reviewer_name TEXT,
            rating NUMERIC(2,1),
            review_summary TEXT,
            sentiment TEXT,
            unix_review_timestamp TIMESTAMP WITHOUT TIME ZONE,
            review_timestamp DATE,
            ingestion_timestamp TIMESTAMP WITHOUT TIME ZONE,
            PRIMARY KEY (reviewer_id, product_id, unix_review_timestamp)
        );
    """).format(sql.Identifier(PROCESSED_TABLE_NAME))

    with conn.cursor() as cur:
        cur.execute(create_table_query)
    conn.commit()
    print(f"Table '{PROCESSED_TABLE_NAME}' created or already exists.")