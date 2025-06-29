# models/staging/stg_review_sentiment.py
import pandas as pd
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer

# Ensure you have the NLTK vader_lexicon downloaded.
# This block runs once per dbt invocation if the lexicon isn't found.
try:
    nltk.data.find('sentiment/vader_lexicon.zip')
except nltk.downloader.DownloadError:
    nltk.download('vader_lexicon', quiet=True) # Use quiet=True to avoid verbose output

analyzer = SentimentIntensityAnalyzer()

def get_sentiment_label(text):
    """Calculates sentiment label for a given text using VADER."""
    if pd.isna(text) or not isinstance(text, str) or text.strip() == '':
        return None # Handle NaN, non-string, or empty strings explicitly
    try:
        score = analyzer.polarity_scores(text)
        if score['compound'] >= 0.05:
            return 'Positive'
        elif score['compound'] <= -0.05:
            return 'Negative'
        else:
            return 'Neutral'
    except Exception: # Catch any other potential errors during sentiment analysis
        return None

def model(dbt, session):
    """
    dbt Python model to calculate sentiment for review text incrementally.
    """
    dbt.config(
        materialized='incremental',
        # Define the unique key for incremental updates (composite key for a review event)
        unique_key=['reviewer_id', 'product_id', 'review_timestamp'],
        schema='public' # Or your preferred staging schema
    )

    df_raw: pd.DataFrame = None # Explicitly type hint and initialize

    # --- Incremental Loading Logic ---
    if dbt.is_incremental:
        max_ingestion_timestamp_query = f"SELECT MAX(ingestion_timestamp) FROM {dbt.this}"
        max_ingestion_timestamp_result = dbt.run_query(max_ingestion_timestamp_query)

        # Extract the timestamp, handling cases where the table is empty or column is NULL
        max_ingestion_timestamp = None
        if not max_ingestion_timestamp_result.empty and max_ingestion_timestamp_result.iloc[0, 0] is not None:
             max_ingestion_timestamp = max_ingestion_timestamp_result.iloc[0, 0]

        # Load all raw data into Pandas for filtering.
        # Note: For dbt-postgres, `to_pandas()` might pull all data before filtering,
        # which can be inefficient for very large sources.
        all_raw_df = dbt.ref('processed_reviews_data').to_pandas()

        if max_ingestion_timestamp:
            # Filter for new records based on ingestion_timestamp
            df_raw = all_raw_df[all_raw_df['ingestion_timestamp'] > max_ingestion_timestamp].copy()
        else:
            # First incremental run, or no existing data, so load all available raw data
            df_raw = all_raw_df.copy() # Use .copy() to ensure it's a distinct DataFrame
    else:
        # --- Full Refresh Loading Logic ---
        # If not an incremental run, load all data from the staging table
        df_raw = dbt.ref('processed_reviews_data').to_pandas()

    # --- Processing and Return ---
    result_df: pd.DataFrame # Declare the variable that will hold the final DataFrame

    if df_raw.empty:
        # If no new data, return an empty DataFrame with the expected schema
        result_df = pd.DataFrame(columns=[
            'reviewer_id',
            'product_id',
            'review_timestamp',
            'sentiment',
            'ingestion_timestamp'
        ])
    else:
        # Apply sentiment analysis
        df_raw['sentiment'] = df_raw['review_text'].apply(get_sentiment_label)

        # Select and reorder columns for the final output
        result_df = df_raw[[
            'reviewer_id',
            'product_id',
            'review_timestamp',
            'sentiment',
            'ingestion_timestamp'
        ]].copy() # Ensure it's a distinct DataFrame

        # Explicitly ensure correct data types for PostgreSQL compatibility
        result_df['reviewer_id'] = result_df['reviewer_id'].astype(str)
        result_df['product_id'] = result_df['product_id'].astype(str)
        # Convert review_timestamp to datetime and then to date if needed, or keep as datetime
        result_df['review_timestamp'] = pd.to_datetime(result_df['review_timestamp'])
        # Replace pandas NaN for sentiment with Python None for cleaner DB storage
        result_df['sentiment'] = result_df['sentiment'].astype(str).replace('nan', None)
        result_df['ingestion_timestamp'] = pd.to_datetime(result_df['ingestion_timestamp'])

    # The function must always return exactly one pandas.DataFrame object
    return result_df