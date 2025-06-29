import pandas as pd
import psycopg2
from psycopg2 import sql
from datetime import datetime

def process_review_data():
    # Database connection parameters
    # Use the service name 'postgres' as the host when running within Docker Compose
    DB_HOST = "postgres"
    DB_NAME = "airflow"
    DB_USER = "airflow"
    DB_PASSWORD = "airflow"
    DB_PORT = "5432"

    RAW_TABLE_NAME = "raw_data_reviews_data"
    PROCESSED_TABLE_NAME = "processed_reviews_data"

    conn = None
    try:
        # Establish connection to PostgreSQL
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT
        )
        cur = conn.cursor()
        print("Successfully connected to PostgreSQL database.")

        # Read existing data from the raw table
        print(f"Reading data from '{RAW_TABLE_NAME}'...")
        query = sql.SQL("SELECT * FROM public.{}").format(sql.Identifier(RAW_TABLE_NAME))
        df = pd.read_sql(query.as_string(conn), conn)
        print(f"Successfully read {len(df)} rows from '{RAW_TABLE_NAME}'.")

        try:
            df['unixreviewtime_dt'] = pd.to_datetime(df['unixreviewtime'], unit='s', errors='coerce')
            print("Converted 'unixreviewtime' to datetime format.")
        except Exception as e:
            print(f"Warning: Could not convert 'unixreviewtime'. Error: {e}")
            df['unixreviewtime_dt'] = None 

        try:
            df['reviewtime_dt'] = pd.to_datetime(df['reviewtime'], format='%m %d, %Y', errors='coerce')
            print("Converted 'reviewtime' to datetime format.")
        except Exception as e:
            print(f"Warning: Could not convert 'reviewtime'. Error: {e}")
            df['reviewtime_dt'] = None

        processed_df = df[[
            'reviewerid', 'asin', 'reviewername', 'helpful', 'reviewtext',
            'overall', 'summary', 'unixreviewtime_dt', 'reviewtime_dt',
            'ingestion_timestamp'
        ]].copy()

        # Rename columns to their desired SQL names if different
        processed_df.rename(columns={
            'reviewerid': 'reviewer_id',
            'asin': 'product_id',
            'reviewername': 'reviewer_name',
            'helpful': 'helpful_votes',
            'reviewtext': 'review_text',
            'overall': 'rating',
            'summary': 'review_summary',
            'unixreviewtime_dt': 'unix_review_timestamp',
            'reviewtime_dt': 'review_timestamp',
            'ingestion_timestamp': 'ingestion_timestamp'
        }, inplace=True)

        print(f"Processed data head:\n{processed_df.head()}")

        # 4. Create the new processed table
        print(f"Creating table '{PROCESSED_TABLE_NAME}' if it does not exist...")
        create_table_query = sql.SQL("""
            CREATE TABLE IF NOT EXISTS public.{} (
                reviewer_id TEXT,
                product_id TEXT,
                reviewer_name TEXT,
                helpful_votes TEXT,
                review_text TEXT,
                rating NUMERIC(2,1),
                review_summary TEXT,
                unix_review_timestamp TIMESTAMP WITHOUT TIME ZONE,
                review_timestamp DATE, -- Or TIMESTAMP if you need time part
                ingestion_timestamp TIMESTAMP WITHOUT TIME ZONE
            );
        """).format(sql.Identifier(PROCESSED_TABLE_NAME))
        cur.execute(create_table_query)
        conn.commit()
        print(f"Table '{PROCESSED_TABLE_NAME}' created or already exists.")

        print(f"Inserting {len(processed_df)} rows into '{PROCESSED_TABLE_NAME}'...")
        
        cols = ['reviewer_id', 'product_id', 'reviewer_name', 'helpful_votes', 'review_text',
                'rating', 'review_summary', 'unix_review_timestamp', 'review_timestamp',
                'ingestion_timestamp']
        
        data_to_insert = [tuple(row.replace({pd.NA: None, pd.NaT: None}).tolist()) for index, row in processed_df[cols].iterrows()]

        insert_query = sql.SQL("""
            INSERT INTO public.{} (
                reviewer_id, product_id, reviewer_name, helpful_votes, review_text,
                rating, review_summary, unix_review_timestamp, review_timestamp,
                ingestion_timestamp
            ) VALUES ({})
        """).format(
            sql.Identifier(PROCESSED_TABLE_NAME),
            sql.SQL(', ').join(sql.Placeholder() * len(cols))
        )

        cur.executemany(insert_query, data_to_insert)
        conn.commit()
        print(f"Successfully inserted {len(processed_df)} rows into '{PROCESSED_TABLE_NAME}'.")

    except Exception as e:
        print(f"An error occurred: {e}")
        if conn:
            conn.rollback()
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
        print("PostgreSQL connection closed.")

if __name__ == "__main__":
    process_review_data()