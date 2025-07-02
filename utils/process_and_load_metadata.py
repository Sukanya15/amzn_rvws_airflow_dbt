import pandas as pd
import ast
import os
from psycopg2 import sql, extras
from datetime import datetime

from db_conn import get_db_connection, create_processed_metadata_table

PROCESSED_TABLE_NAME = "processed_metadata_category"

current_script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_script_dir)
CSV_FILES_DIR = os.path.join(project_root, 'csv_files')

METADATA_CSV_PATH = os.path.join(CSV_FILES_DIR, 'metadata_category_clothing_shoes_and_jewelry_only.csv')


def process_and_load_metadata():
    conn = None
    try:
        conn = get_db_connection()
        conn.autocommit = False
        print("Successfully connected to PostgreSQL database.")

        create_processed_metadata_table(conn, PROCESSED_TABLE_NAME)
        
        print(f"Reading and processing metadata from {METADATA_CSV_PATH} in chunks...")

        chunk_size = 10000 
        rows_processed = 0

        with conn.cursor() as cur:
            for chunk_df in pd.read_csv(METADATA_CSV_PATH, chunksize=chunk_size):
                print(f"  Processing a chunk of {len(chunk_df)} rows from CSV...")

                for col in ['salesrank', 'imurl', 'categories', 'title', 'description', 'price', 'related', 'brand']:
                    if col in chunk_df.columns:
                        chunk_df[col] = chunk_df[col].astype(str).replace({'nan': None})
                
                chunk_df.dropna(subset=['asin'], inplace=True) 

                if 'sales_category' not in chunk_df.columns:
                    chunk_df['sales_category'] = None
                if 'sales_rank' not in chunk_df.columns:
                    chunk_df['sales_rank'] = None

                for index, row in chunk_df.iterrows():
                    salesrank_str = row['salesrank']
                    if pd.notna(salesrank_str) and salesrank_str.strip() != '{}':
                        try:
                            salesrank_dict = ast.literal_eval(salesrank_str)
                            if isinstance(salesrank_dict, dict) and salesrank_dict:
                                key, value = next(iter(salesrank_dict.items()))
                                chunk_df.at[index, 'sales_category'] = key
                                try:
                                    chunk_df.at[index, 'sales_rank'] = int(value)
                                except (ValueError, TypeError):
                                    chunk_df.at[index, 'sales_rank'] = None
                        except (ValueError, SyntaxError) as e:
                            print(f"Warning: Could not parse salesrank '{salesrank_str}' at index {index} for chunk. Error: {e}")
                    elif salesrank_str == '{}':
                        chunk_df.at[index, 'sales_category'] = None
                        chunk_df.at[index, 'sales_rank'] = None

                if 'first_category' not in chunk_df.columns:
                    chunk_df['first_category'] = None
                for index, row in chunk_df.iterrows():
                    categories_str = row['categories']
                    if pd.notna(categories_str):
                        try:
                            categories_list = ast.literal_eval(categories_str)
                            if isinstance(categories_list, list) and categories_list:
                                if categories_list[0] and isinstance(categories_list[0], list):
                                    chunk_df.at[index, 'first_category'] = categories_list[0][0]
                                elif categories_list[0] and isinstance(categories_list[0], str):
                                    chunk_df.at[index, 'first_category'] = categories_list[0]
                        except (ValueError, SyntaxError) as e:
                            print(f"Warning: Could not parse categories '{categories_str}' at index {index} for chunk. Error: {e}")

                chunk_df['ingestion_timestamp'] = datetime.now()

                processed_chunk_df = chunk_df[[
                    'metadataid', 'asin', 'imurl', 'sales_category', 'sales_rank', 'first_category',
                    'title', 'description', 'price', 'related', 'brand', 'ingestion_timestamp'
                ]].copy()

                processed_chunk_df.rename(columns={
                    'metadataid': 'metadata_id',
                    'asin': 'product_id',
                    'imurl': 'image_url',
                    'title': 'product_title',
                    'description': 'product_description',
                    'price': 'product_price',
                    'related': 'related_products',
                    'brand': 'product_brand',
                }, inplace=True)
                    
                target_cols = [
                    'metadata_id', 'product_id', 'image_url', 'sales_category', 'sales_rank',
                    'first_category', 'product_title', 'product_description', 'product_price',
                    'related_products', 'product_brand', 'ingestion_timestamp'
                ]
                
                data_to_insert = []
                for index, row in processed_chunk_df.iterrows():
                    try:
                        row_values = [row[col] for col in target_cols]

                        price_index = target_cols.index('product_price')
                        original_price = row_values[price_index]
                        if pd.isna(original_price) or original_price is None:
                            row_values[price_index] = None
                        else:
                            try:
                                price_str = str(original_price).strip().replace('$', '').replace(',', '')
                                row_values[price_index] = float(price_str)
                            except (ValueError, TypeError):
                                print(f"Warning: Could not convert price '{original_price}' to float for row {index}. Setting to None.")
                                row_values[price_index] = None

                        cleaned_row = [None if pd.isna(x) or pd.isnull(x) else x for x in row_values]
                        data_to_insert.append(tuple(cleaned_row))
                    except Exception as e:
                        print(f"Error preparing row {index} for insertion in chunk: {e}. Row data: {row.to_dict()}")
                        continue
                
                if data_to_insert:
                    insert_sql = sql.SQL("""
                        INSERT INTO public.{} ({}) VALUES %s
                        ON CONFLICT (product_id) DO NOTHING;
                    """).format(
                        sql.Identifier(PROCESSED_TABLE_NAME),
                        sql.SQL(', ').join(map(sql.Identifier, target_cols))
                    )
                    extras.execute_values(cur, insert_sql, data_to_insert, page_size=chunk_size)
                    rows_processed += len(data_to_insert)
                    print(f"  Inserted {len(data_to_insert)} rows from chunk. Total processed: {rows_processed}")
                else:
                    print("  No rows to insert for this chunk after processing.")

            conn.commit()
            print(f"Finished processing and loading metadata. Total rows inserted: {rows_processed}")

    except FileNotFoundError:
        print(f"Error: Metadata CSV file not found at {METADATA_CSV_PATH}")
    except Exception as e:
        print(f"An unexpected error occurred during metadata processing: {e}")
        if conn:
            conn.rollback()
        raise

    finally:
        if conn:
            conn.close()
        print("PostgreSQL connection closed.")

if __name__ == "__main__":
    print("Starting metadata processing and loading...")
    process_and_load_metadata()