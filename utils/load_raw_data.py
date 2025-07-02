import pandas as pd
import os
from psycopg2 import extras
from datetime import datetime
from db_conn import get_db_connection, create_raw_tables

current_script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_script_dir)
CSV_FILES_DIR = os.path.join(project_root, 'csv_files')

REVIEWS_CSV_PATH = os.path.join(CSV_FILES_DIR, 'reviews_Clothing_Shoes_and_Jewelry_5.csv')
METADATA_CSV_PATH = os.path.join(CSV_FILES_DIR, 'metadata_category_clothing_shoes_and_jewelry_only.csv')


def load_metadata_to_raw(conn, csv_path):
    """Loads data from the metadata CSV into raw_data_metadata_category table."""
    print(f"Loading metadata from {csv_path} to raw_data_metadata_category...")
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
            print(f"  Loaded/Updated {len(values)} rows from metadata CSV chunk.")
        print("Finished loading metadata to raw table.")
    except FileNotFoundError:
        print(f"Error: Metadata CSV file not found at {csv_path}")
    except Exception as e:
        print(f"Error loading metadata to raw table: {e}")
        raise

def load_reviews_to_raw(conn, csv_path):
    """Loads data from the reviews CSV into raw_data_reviews_data table."""
    print(f"Loading reviews from {csv_path} to raw_data_reviews_data...")
    try:
        chunk_size = 10000
        for chunk_df in pd.read_csv(csv_path, chunksize=chunk_size):
            chunk_df['overall'] = pd.to_numeric(chunk_df['overall'], errors='coerce').fillna(0).round(1)
            chunk_df['unixreviewtime'] = pd.to_numeric(chunk_df['unixreviewtime'], errors='coerce').fillna(0).astype(int)

            chunk_df.dropna(subset=['reviewerid', 'asin', 'unixreviewtime'], inplace=True)

            for col in ['reviewername', 'helpful', 'reviewtext', 'summary', 'reviewtime']:
                if col in chunk_df.columns:
                    chunk_df[col] = chunk_df[col].astype(str).replace({'nan': None})


            values = []
            for index, row in chunk_df.iterrows():
                values.append((
                    row['reviewerid'],
                    row['asin'],
                    row.get('reviewername'),
                    row.get('helpful'),
                    row.get('reviewtext'),
                    row['overall'],
                    row.get('summary'),
                    row['unixreviewtime'],
                    row.get('reviewtime')
                ))

            if values:
                columns = [
                    "reviewerid", "asin", "reviewername", "helpful",
                    "reviewtext", "overall", "summary", "unixreviewtime", "reviewtime"
                ]
                insert_sql = f"""
                INSERT INTO raw_data_reviews_data ({', '.join(columns)})
                VALUES %s
                ON CONFLICT (reviewerid, asin, unixreviewtime) DO UPDATE SET
                    reviewername = EXCLUDED.reviewername,
                    helpful = EXCLUDED.helpful,
                    reviewtext = EXCLUDED.reviewtext,
                    overall = EXCLUDED.overall,
                    summary = EXCLUDED.summary,
                    reviewtime = EXCLUDED.reviewtime,
                    ingestion_timestamp = CURRENT_TIMESTAMP;
                """
                with conn.cursor() as cur:
                    extras.execute_values(cur, insert_sql, values, page_size=chunk_size)
            print(f"  Loaded/Updated {len(values)} rows from reviews CSV chunk.")
        print("Finished loading reviews to raw table.")
    except FileNotFoundError:
        print(f"Error: Reviews CSV file not found at {csv_path}")
    except Exception as e:
        print(f"Error loading reviews to raw table: {e}")
        raise

def load_raw_data_main():
    conn = None
    try:
        conn = get_db_connection()
        create_raw_tables(conn)

        load_metadata_to_raw(conn, METADATA_CSV_PATH)
        conn.commit()

        load_reviews_to_raw(conn, REVIEWS_CSV_PATH)
        conn.commit()

        print("Raw data loading process complete.")

    except Exception as e:
        conn.rollback()
        print(f"An error occurred during raw data loading: {e}")
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    print("Starting raw data ingestion...")
    load_raw_data_main()