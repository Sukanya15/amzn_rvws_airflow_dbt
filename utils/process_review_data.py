import pandas as pd
import psycopg2
from psycopg2 import sql, extras
from datetime import datetime

from db_conn import get_db_connection, create_processed_reviews_table

RAW_TABLE_NAME = "raw_data_reviews_data"
PROCESSED_TABLE_NAME = "processed_reviews_data"

def process_review_data():
    conn = None
    try:
        conn = get_db_connection()
        print("Successfully connected to PostgreSQL database.")

        create_processed_reviews_table(conn, PROCESSED_TABLE_NAME)

        print(f"Processing data from '{RAW_TABLE_NAME}' in chunks...")

        chunk_size = 5000 

        raw_cols_to_read = [
            'reviewerid', 'asin', 'reviewername',
            'overall', 'summary', 'unixreviewtime', 'reviewtime',
            'ingestion_timestamp'
        ]

        with conn.cursor() as count_cur:
            count_query = sql.SQL("SELECT COUNT(*) FROM public.{}").format(
                sql.Identifier(RAW_TABLE_NAME)
            )
            count_cur.execute(count_query)
            total_rows = count_cur.fetchone()[0]
            print(f"Total rows in '{RAW_TABLE_NAME}': {total_rows}")

        with conn.cursor(name="reviews_fetch_cursor") as fetch_cur:
            fetch_cur.itersize = chunk_size

            select_query = sql.SQL("SELECT {} FROM public.{}").format(
                sql.SQL(', ').join(map(sql.Identifier, raw_cols_to_read)),
                sql.Identifier(RAW_TABLE_NAME)
            )
            fetch_cur.execute(select_query)

            column_names = [desc[0] for desc in fetch_cur.description]

            rows_processed = 0
            iterator = 0
            while True:
                iterator = iterator + 1
                print(f"Fetching next {chunk_size} rows... {iterator} time")
                try:
                    chunk_rows = fetch_cur.fetchmany(chunk_size)
                    if not chunk_rows:
                        print("DEBUG: No more rows to fetch (empty list returned). Breaking loop.")
                        break

                except psycopg2.ProgrammingError as e:
                    if "no results to fetch" in str(e).lower():
                        print(f"DEBUG: Caught psycopg2.ProgrammingError 'no results to fetch'. Assuming end of data. Error: {e}")
                        break
                    else:
                        print(f"Error during fetchmany (ProgrammingError): {e}")
                        raise

                except Exception as e:
                    print(f"Error during fetchmany (unexpected exception): {e}")
                    raise

                chunk_df = pd.DataFrame(chunk_rows, columns=column_names)

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

                processed_chunk_df = chunk_df[[
                    'reviewerid', 'asin', 'reviewername',
                    'rating', 'summary', 'unix_review_timestamp', 'review_timestamp',
                    'ingestion_timestamp'
                ]].copy()

                processed_chunk_df.rename(columns={
                    'reviewerid': 'reviewer_id',
                    'asin': 'product_id',
                    'reviewername': 'reviewer_name',
                    'summary': 'review_summary'
                }, inplace=True)

                target_cols = [
                    'reviewer_id', 'product_id', 'reviewer_name',
                    'rating', 'review_summary', 'unix_review_timestamp', 'review_timestamp',
                    'ingestion_timestamp'
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
                    with conn.cursor() as insert_cur:
                        insert_sql = sql.SQL("""
                            INSERT INTO public.{} ({}) VALUES %s
                            ON CONFLICT (reviewer_id, product_id, unix_review_timestamp) DO NOTHING;
                        """).format(
                            sql.Identifier(PROCESSED_TABLE_NAME),
                            sql.SQL(', ').join(map(sql.Identifier, target_cols))
                        )
                        extras.execute_values(insert_cur, insert_sql, data_to_insert, page_size=chunk_size)
                    
                    rows_processed += len(data_to_insert)
                    print(f"  Processed and inserted {len(data_to_insert)} rows. Total rows processed: {rows_processed}")
            
            conn.commit()

            print(f"Finished processing reviews. Total rows inserted: {rows_processed}")

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
    print("Starting review data processing...")
    process_review_data()