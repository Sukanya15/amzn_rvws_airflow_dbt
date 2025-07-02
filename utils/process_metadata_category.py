import pandas as pd
import ast
import psycopg2
from psycopg2 import sql, extras
from db_conn import get_db_connection, create_processed_metadata_table

RAW_TABLE_NAME = "raw_data_metadata_category"
PROCESSED_TABLE_NAME = "processed_metadata_category"

def process_metadata_data():
    conn = None
    cur = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        print("Successfully connected to PostgreSQL database.")

        print(f"Reading data from '{RAW_TABLE_NAME}'...")
        query = sql.SQL("SELECT * FROM public.{}").format(sql.Identifier(RAW_TABLE_NAME))
        df = pd.read_sql(query.as_string(conn), conn)
        print(f"Successfully read {len(df)} rows from '{RAW_TABLE_NAME}'.")

        df['sales_category'] = None
        df['sales_rank'] = None

        for index, row in df.iterrows():
            salesrank_str = row['salesrank']
            if pd.notna(salesrank_str) and salesrank_str.strip() != '{}':
                try:
                    salesrank_dict = ast.literal_eval(salesrank_str)
                    if isinstance(salesrank_dict, dict) and salesrank_dict:
                        key, value = next(iter(salesrank_dict.items()))
                        df.at[index, 'sales_category'] = key
                        try:
                            df.at[index, 'sales_rank'] = int(value)
                        except (ValueError, TypeError):
                            df.at[index, 'sales_rank'] = None
                except (ValueError, SyntaxError) as e:
                    print(f"Warning: Could not parse salesrank '{salesrank_str}' at index {index}. Error: {e}")
            elif salesrank_str == '{}':
                df.at[index, 'sales_category'] = None
                df.at[index, 'sales_rank'] = None

        print("Processed 'salesrank' into 'sales_category' and 'sales_rank'.")

        df['first_category'] = None

        for index, row in df.iterrows():
            categories_str = row['categories']
            if pd.notna(categories_str):
                try:
                    categories_list = ast.literal_eval(categories_str)
                    if isinstance(categories_list, list) and categories_list:
                        if categories_list[0] and isinstance(categories_list[0], list):
                            df.at[index, 'first_category'] = categories_list[0][0]
                        elif categories_list[0] and isinstance(categories_list[0], str):
                             df.at[index, 'first_category'] = categories_list[0]
                except (ValueError, SyntaxError) as e:
                    print(f"Warning: Could not parse categories '{categories_str}' at index {index}. Error: {e}")

        print("Processed 'categories' to extract 'first_category'.")

        processed_df = df[[
            'metadataid', 'asin', 'imurl', 'sales_category', 'sales_rank', 'first_category',
            'title', 'description', 'price', 'related', 'brand', 'ingestion_timestamp'
        ]].copy()

        processed_df.rename(columns={
            'metadataid': 'metadata_id',
            'asin': 'product_id',
            'imurl': 'image_url',
            'sales_category': 'sales_category',
            'sales_rank': 'sales_rank',
            'first_category': 'first_category',
            'title': 'product_title',
            'description': 'product_description',
            'price': 'product_price',
            'related': 'related_products',
            'brand': 'product_brand',
            'ingestion_timestamp': 'ingestion_timestamp'
        }, inplace=True)

        print(f"Processed data head:\n{processed_df.head()}")

        create_processed_metadata_table(conn, PROCESSED_TABLE_NAME)

        print(f"Preparing {len(processed_df)} rows for insertion into '{PROCESSED_TABLE_NAME}'...")

        target_cols = [
            'metadata_id', 'product_id', 'image_url', 'sales_category', 'sales_rank',
            'first_category', 'product_title', 'product_description', 'product_price',
            'related_products', 'product_brand', 'ingestion_timestamp'
        ]

        data_to_insert = []
        for index, row in processed_df.iterrows():
            try:
                row_values = [row[col] for col in target_cols]

                price_index = target_cols.index('product_price')
                original_price = row_values[price_index]
                if pd.isna(original_price):
                    row_values[price_index] = None
                else:
                    try:
                        row_values[price_index] = float(original_price)
                    except (ValueError, TypeError):
                        row_values[price_index] = None

                cleaned_row = [None if pd.isna(x) else x for x in row_values]
                data_to_insert.append(tuple(cleaned_row))
            except Exception as e:
                print(f"Error preparing row {index} for insertion: {e}. Row data: {row.to_dict()}")
                continue

        print(f"Inserting {len(data_to_insert)} rows into '{PROCESSED_TABLE_NAME}' using execute_values...")

        insert_sql = sql.SQL("""
            INSERT INTO public.{} ({}) VALUES %s
            ON CONFLICT (product_id) DO NOTHING;
        """).format(
            sql.Identifier(PROCESSED_TABLE_NAME),
            sql.SQL(', ').join(map(sql.Identifier, target_cols))
        )

        extras.execute_values(cur, insert_sql, data_to_insert, page_size=10000)
        conn.commit()
        print(f"Successfully inserted {len(data_to_insert)} rows into '{PROCESSED_TABLE_NAME}'.")

    except Exception as e:
        print(f"An error occurred during metadata processing: {e}")
        if conn:
            conn.rollback()
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
        print("PostgreSQL connection closed.")

if __name__ == "__main__":
    print("Starting metadata processing...")
    process_metadata_data()