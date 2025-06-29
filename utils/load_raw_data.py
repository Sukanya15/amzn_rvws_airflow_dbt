import pandas as pd
import psycopg2
from psycopg2 import extras
import os
from datetime import datetime

# --- Configuration ---
DB_HOST = os.getenv('ARDWH_HOST', '172.19.0.2')
DB_NAME = os.getenv('ARDWH_DB', 'amazon_reviews_dwh')
DB_USER = os.getenv('ARDWH_USER', 'airflow')
DB_PASSWORD = os.getenv('ARDWH_PASSWORD', 'airflow')
DB_PORT = os.getenv('ARDWH_PORT', '5432')

# Define paths to your CSV files
current_script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_script_dir)
CSV_FILES_DIR = os.path.join(project_root, 'csv_files')

REVIEWS_CSV_PATH = os.path.join(CSV_FILES_DIR, 'reviews_Clothing_Shoes_and_Jewelry_5.csv')
METADATA_CSV_PATH = os.path.join(CSV_FILES_DIR, 'metadata_category_clothing_shoes_and_jewelry_only.csv')


# --- DB Connection ---
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
        conn.autocommit = True
        print("Successfully connected to the database.")
        return conn
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        raise

def create_raw_tables(conn):
    with conn.cursor() as cur:
        # Table for raw_data_metadata_category
        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw_data_metadata_category (
                metadataid BIGINT,
                asin TEXT PRIMARY KEY,
                salesrank TEXT,
                imurl TEXT,
                categories TEXT,
                title TEXT,
                description TEXT,
                price TEXT,
                related TEXT,
                brand TEXT,
                ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        # Table for raw_data_reviews_data
        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw_data_reviews_data (
                reviewerID TEXT,
                asin TEXT,
                reviewerName TEXT,
                helpful TEXT,
                reviewText TEXT,
                overall NUMERIC(2,1),
                summary TEXT,
                unixReviewTime BIGINT,
                reviewTime TEXT,
                ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (reviewerID, asin, unixReviewTime)
            );
        """)
    print("Raw tables created or already exist.")

# --- Data Loading Functions ---
def load_metadata_to_raw(conn, csv_path):
    """Loads data from the metadata CSV into stg_metadata_category table."""
    print(f"Loading metadata from {csv_path} to stg_metadata_category...")
    try:
        chunk_size = 10000 
        for chunk_df in pd.read_csv(csv_path, chunksize=chunk_size):
            for col in ['salesrank', 'imurl', 'categories', 'title', 'description', 'price', 'related', 'brand']:
                if col in chunk_df.columns:
                    chunk_df[col] = chunk_df[col].astype(str).replace({'nan': None})
            
            chunk_df.dropna(subset=['asin'], inplace=True)

            values = []
            for index, row in chunk_df.iterrows():
                values.append((
                    row.get('metadataid'),
                    row['asin'],
                    row.get('salesrank'),
                    row.get('imurl'),
                    row.get('categories'),
                    row.get('title'),
                    row.get('description'),
                    row.get('price'),
                    row.get('related'),
                    row.get('brand')
                ))

            if values:
                columns = [
                    "metadataid", "asin", "salesrank", "imurl", "categories",
                    "title", "description", "price", "related", "brand"
                ]
                insert_sql = f"""
                INSERT INTO raw_data_metadata_category ({', '.join(columns)})
                VALUES %s
                ON CONFLICT (asin) DO UPDATE SET
                    metadataid = EXCLUDED.metadataid,
                    salesrank = EXCLUDED.salesrank,
                    imurl = EXCLUDED.imurl,
                    categories = EXCLUDED.categories,
                    title = EXCLUDED.title,
                    description = EXCLUDED.description,
                    price = EXCLUDED.price,
                    related = EXCLUDED.related,
                    brand = EXCLUDED.brand,
                    ingestion_timestamp = CURRENT_TIMESTAMP;
                """
                with conn.cursor() as cur:
                    extras.execute_values(cur, insert_sql, values, page_size=chunk_size)
                conn.commit()
            print(f"  Loaded/Updated {len(values)} rows from metadata CSV chunk.")
        print("Finished loading metadata to raw table.")
    except FileNotFoundError:
        print(f"Error: Metadata CSV file not found at {csv_path}")
    except Exception as e:
        print(f"Error loading metadata to raw table: {e}")
        conn.rollback()
        raise

def load_reviews_to_raw(conn, csv_path):
    """Loads data from the reviews CSV into raw_data_reviews_data table."""
    print(f"Loading reviews from {csv_path} to raw_data_reviews_data...")
    try:
        chunk_size = 10000
        for chunk_df in pd.read_csv(csv_path, chunksize=chunk_size):
            chunk_df['overall'] = pd.to_numeric(chunk_df['overall'], errors='coerce').fillna(0).round(1)
            chunk_df['unixReviewTime'] = pd.to_numeric(chunk_df['unixReviewTime'], errors='coerce').fillna(0).astype(int)

            chunk_df.dropna(subset=['reviewerID', 'asin', 'unixReviewTime'], inplace=True)
            
            for col in ['reviewerName', 'helpful', 'reviewText', 'summary', 'reviewTime']:
                if col in chunk_df.columns:
                    chunk_df[col] = chunk_df[col].astype(str).replace({'nan': None})


            values = []
            for index, row in chunk_df.iterrows():
                values.append((
                    row['reviewerID'],
                    row['asin'],
                    row.get('reviewerName'),
                    row.get('helpful'),
                    row.get('reviewText'),
                    row['overall'],
                    row.get('summary'),
                    row['unixReviewTime'],
                    row.get('reviewTime')
                ))
            
            if values:
                columns = [
                    "reviewerID", "asin", "reviewerName", "helpful",
                    "reviewText", "overall", "summary", "unixReviewTime", "reviewTime"
                ]
                insert_sql = f"""
                INSERT INTO raw_data_reviews_data ({', '.join(columns)})
                VALUES %s
                ON CONFLICT (reviewerID, asin, unixReviewTime) DO UPDATE SET
                    reviewerName = EXCLUDED.reviewerName,
                    helpful = EXCLUDED.helpful,
                    reviewText = EXCLUDED.reviewText,
                    overall = EXCLUDED.overall,
                    summary = EXCLUDED.summary,
                    reviewTime = EXCLUDED.reviewTime,
                    ingestion_timestamp = CURRENT_TIMESTAMP;
                """
                with conn.cursor() as cur:
                    extras.execute_values(cur, insert_sql, values, page_size=chunk_size)
                conn.commit()
            print(f"  Loaded/Updated {len(values)} rows from reviews CSV chunk.")
        print("Finished loading reviews to raw table.")
    except FileNotFoundError:
        print(f"Error: Reviews CSV file not found at {csv_path}")
    except Exception as e:
        print(f"Error loading reviews to raw table: {e}")
        conn.rollback()
        raise

# --- Main Execution for Raw Load ---
def load_raw_data_main():
    conn = None
    try:
        conn = get_db_connection()
        create_raw_tables(conn)
        
        load_metadata_to_raw(conn, METADATA_CSV_PATH)
        load_reviews_to_raw(conn, REVIEWS_CSV_PATH)

        print("Raw data loading process complete.")

    except Exception as e:
        print(f"An error occurred during raw data loading: {e}")
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    print("Starting raw data ingestion...")
    load_raw_data_main()