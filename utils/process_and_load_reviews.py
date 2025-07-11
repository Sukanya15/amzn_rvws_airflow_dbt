import pandas as pd
from psycopg2 import sql, extras
from datetime import datetime
import os
import requests
import numpy as np

from db_conn import get_db_connection, create_processed_reviews_table

PROCESSED_TABLE_NAME = "processed_reviews_data"

current_script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_script_dir)
CSV_FILES_DIR = os.path.join(project_root, 'csv_files')

REVIEWS_CSV_PATH = os.path.join(CSV_FILES_DIR, 'reviews_Clothing_Shoes_and_Jewelry_5.csv')

SENTIMENT_SERVICE_URL = os.getenv('SENTIMENT_API_URL', 'http://127.0.0.1:5001/sentiment')

API_BATCH_SIZE = 500

def get_sentiments_from_service_batched(texts_list):
    if not texts_list:
        return []
    try:
        response = requests.post(
            SENTIMENT_SERVICE_URL,
            json={'texts': texts_list},
            timeout=30
        )
        response.raise_for_status()
        sentiment_data = response.json()
        return sentiment_data.get('sentiments', [])
    except requests.exceptions.RequestException as e:
        print(f"Error connecting to sentiment service for batch. Error: {e}")
        return [None] * len(texts_list)
    
def process_and_load_reviews():
    conn = None
    try:
        conn = get_db_connection()
        conn.autocommit = False
        print("Successfully connected to PostgreSQL database.")

        create_processed_reviews_table(conn, PROCESSED_TABLE_NAME)

        print(f"Reading and processing reviews from {REVIEWS_CSV_PATH} in chunks...")

        chunk_size = 10000
        rows_processed = 0

        with conn.cursor() as cur:
            for chunk_df in pd.read_csv(REVIEWS_CSV_PATH, chunksize=chunk_size):
                chunk_df['overall'] = pd.to_numeric(chunk_df['overall'], errors='coerce').fillna(0).round(1)
                chunk_df['unixreviewtime'] = pd.to_numeric(chunk_df['unixreviewtime'], errors='coerce').fillna(0).astype(int)

                chunk_df.dropna(subset=['reviewerid', 'asin', 'unixreviewtime'], inplace=True)

                for col in ['reviewername', 'helpful', 'reviewtext', 'summary', 'reviewtime']:
                    if col in chunk_df.columns:
                        chunk_df[col] = chunk_df[col].astype(str).replace({'nan': None})

                try:
                    chunk_df['unix_review_timestamp'] = pd.to_datetime(chunk_df['unixreviewtime'], unit='s', errors='coerce')
                except Exception as e:
                    print(f"Warning: Could not convert 'unixreviewtime' in chunk. Error: {e}")
                    chunk_df['unix_review_timestamp'] = pd.NaT

                try:
                    chunk_df['review_timestamp'] = pd.to_datetime(chunk_df['reviewtime'], format='%m %d, %Y', errors='coerce').dt.date
                except Exception as e:
                    print(f"Warning: Could not convert 'reviewtime' in chunk. Error: {e}")
                    chunk_df['review_timestamp'] = None

                chunk_df['rating'] = pd.to_numeric(chunk_df['overall'], errors='coerce').round(1)

                chunk_df['ingestion_timestamp'] = datetime.now()

                if 'summary' not in chunk_df.columns:
                    print("Warning: 'summary' column not found, falling back to 'reviewtext' for sentiment.")
                    chunk_df['summary'] = chunk_df['reviewtext']
                chunk_df['summary'] = chunk_df['summary'].astype(str).replace({'nan': None})
                
                texts_to_analyze = chunk_df['summary'].tolist()
                
                all_sentiments_for_chunk = []
                for i in range(0, len(texts_to_analyze), API_BATCH_SIZE):
                    batch_texts = texts_to_analyze[i:i + API_BATCH_SIZE]
                    batch_texts_cleaned = [t if t is not None else "" for t in batch_texts]
                    
                    print(f"  Sending batch {int(i/API_BATCH_SIZE) + 1} of {len(texts_to_analyze) // API_BATCH_SIZE + (1 if len(texts_to_analyze) % API_BATCH_SIZE else 0)} to sentiment service...")
                    sentiments_batch = get_sentiments_from_service_batched(batch_texts_cleaned)
                    all_sentiments_for_chunk.extend(sentiments_batch)
                
                chunk_df['sentiment'] = all_sentiments_for_chunk
                
                print("Sentiment analysis for chunk completed.")

                chunk_df['sentiment'] = chunk_df['sentiment'].apply(lambda x: None if (pd.isna(x) or pd.isnull(x) or str(x).lower() == 'none') else x)

                # chunk_df['sentiment'] =  'Positive' # chunk_df['summary'].apply(get_sentiment_label)

                # chunk_df['sentiment'] = chunk_df['sentiment'].astype(str).replace('nan', None)

                processed_chunk_df = chunk_df[[
                    'reviewerid', 'asin', 'reviewername','overall', 
                    'summary', 'sentiment', 'unix_review_timestamp', 
                    'review_timestamp', 'ingestion_timestamp'
                ]].copy()

                processed_chunk_df.rename(columns={
                    'reviewerid': 'reviewer_id',
                    'asin': 'product_id',
                    'reviewername': 'reviewer_name',
                    'overall': 'rating',
                    'summary': 'review_summary'
                }, inplace=True)

                target_cols = [
                    'reviewer_id', 'product_id', 'reviewer_name','rating', 
                    'review_summary', 'sentiment', 'unix_review_timestamp', 
                    'review_timestamp', 'ingestion_timestamp'
                ]

                data_to_insert = []
                for index, row in processed_chunk_df.iterrows():
                    try:
                        row_values = [row[col] for col in target_cols]
                        cleaned_row = [None if pd.isna(x) or pd.isnull(x) else x for x in row_values]
                        data_to_insert.append(tuple(cleaned_row))
                    except Exception as e:
                        print(f"Error preparing row {index} for insertion in chunk: {e}. Row data: {row.to_dict()}")
                        continue

                if data_to_insert:
                    insert_sql = sql.SQL("""
                        INSERT INTO public.{} ({}) VALUES %s
                        ON CONFLICT (reviewer_id, product_id, unix_review_timestamp) DO NOTHING;
                    """).format(
                        sql.Identifier(PROCESSED_TABLE_NAME),
                        sql.SQL(', ').join(map(sql.Identifier, target_cols))
                    )
                    extras.execute_values(cur, insert_sql, data_to_insert, page_size=chunk_size)
                    rows_processed += len(data_to_insert)
                    print(f"  Processed and inserted {len(data_to_insert)} rows from chunk. Total: {rows_processed}")

            conn.commit()
            print(f"Finished processing and loading reviews. Total rows inserted: {rows_processed}")

    except FileNotFoundError:
        print(f"Error: Reviews CSV file not found at {REVIEWS_CSV_PATH}")
    except Exception as e:
        print(f"An unexpected error occurred during review data processing: {e}")
        if conn:
            conn.rollback()
        raise

    finally:
        if conn:
            conn.close()
        print("PostgreSQL connection closed.")

if __name__ == "__main__":
    print("Starting review data processing and loading...")
    process_and_load_reviews()