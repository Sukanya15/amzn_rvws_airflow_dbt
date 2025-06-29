import pandas as pd
import psycopg2
from psycopg2 import sql
import ast

def process_metadata_data():
    # Database connection parameters
    DB_HOST = "postgres"
    DB_NAME = "airflow"
    DB_USER = "airflow"
    DB_PASSWORD = "airflow"
    DB_PORT = "5432"

    RAW_TABLE_NAME = "raw_data_metadata_category"
    PROCESSED_TABLE_NAME = "processed_metadata_category"

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

        # Process 'salesrank' column
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
                        df.at[index, 'sales_rank'] = int(value)
                except (ValueError, SyntaxError) as e:
                    print(f"Warning: Could not parse salesrank '{salesrank_str}' at index {index}. Error: {e}")
            elif salesrank_str == '{}':
                df.at[index, 'sales_category'] = None
                df.at[index, 'sales_rank'] = None

        print("Processed 'salesrank' into 'sales_category' and 'sales_rank'.")

        # Process 'categories' column
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

        # Rename columns to their desired SQL names if different
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
                ingestion_timestamp TIMESTAMP WITHOUT TIME ZONE,
                
            );
        """).format(sql.Identifier(PROCESSED_TABLE_NAME))
        cur.execute(create_table_query)
        conn.commit()
        print(f"Table '{PROCESSED_TABLE_NAME}' created or already exists.")

        # 5. Insert cleansed data into the new processed table
        print(f"Inserting {len(processed_df)} rows into '{PROCESSED_TABLE_NAME}'...")
        
        # Define the columns for insertion explicitly
        cols = [
            'metadataid', 'asin', 'imurl', 'sales_category', 'sales_rank', 'first_category',
            'title', 'description', 'price', 'related', 'brand', 'ingestion_timestamp'
        ]
        
        data_to_insert = []
        for index, row in processed_df[cols].iterrows():
            try:
                row_list = row.tolist()
                if 'price' in row and pd.isna(row['price']):
                     row_list[cols.index('price')] = None
                elif 'price' in row and not pd.api.types.is_numeric_dtype(type(row['price'])):
                    try:
                        row_list[cols.index('price')] = float(row['price'])
                    except ValueError:
                        row_list[cols.index('price')] = None
                
                cleaned_row = [None if pd.isna(x) else x for x in row_list]
                data_to_insert.append(tuple(cleaned_row))
            except Exception as e:
                print(f"Error preparing row {index} for insertion: {e}. Row: {row}")
                continue 


        insert_query = sql.SQL("""
            INSERT INTO public.{} ({}) VALUES ({})
            ON CONFLICT (asin) DO NOTHING; -- Assuming 'asin' is unique and you want to skip duplicates
        """).format(
            sql.Identifier(PROCESSED_TABLE_NAME),
            sql.SQL(', ').join(map(sql.Identifier, cols)),
            sql.SQL(', ').join(sql.Placeholder() * len(cols))
        )

        cur.executemany(insert_query, data_to_insert)
        conn.commit()
        print(f"Successfully inserted {len(data_to_insert)} rows into '{PROCESSED_TABLE_NAME}'.")

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
    process_metadata_data()