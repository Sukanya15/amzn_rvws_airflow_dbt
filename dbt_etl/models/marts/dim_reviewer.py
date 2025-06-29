# # models/marts/dim_reviewer.py
# import pandas as pd
# import nltk
# from nltk.sentiment.vader import SentimentIntensityAnalyzer

# # Ensure you have the NLTK vader_lexicon downloaded.
# # This part should ideally be handled outside the model's runtime if possible
# # (e.g., in your Dockerfile, CI/CD setup, or a pre-job hook).
# # If it must be in the model, add a check to only download if not present.
# try:
#     nltk.data.find('sentiment/vader_lexicon.zip')
# except nltk.downloader.DownloadError:
#     nltk.download('vader_lexicon', quiet=True) # Use quiet=True to avoid verbose output on every run

# analyzer = SentimentIntensityAnalyzer()

# def model(dbt, session):
#     # Configure the materialization for this model
#     dbt.config(
#         materialized='table', # This will create a physical table in your database
#         schema='public'       # The schema where the table will be created
#     )

#     # Read from the staging review data using dbt.ref()
#     # .to_pandas() loads the entire result set into a Pandas DataFrame
#     df = dbt.ref('processed_reviews_data').to_pandas()

#     # Data Standardization and Cleaning
#     # 1. Fill missing reviewer_name
#     df['reviewer_name'] = df['reviewer_name'].fillna('Unknown Reviewer')

#     # 2. Sentiment analysis function using NLTK VADER
#     def get_sentiment_label(text):
#         if pd.isna(text) or not isinstance(text, str) or text.strip() == '':
#             return None # Handle NaN, non-string, or empty strings
#         try:
#             score = analyzer.polarity_scores(text)
#             if score['compound'] >= 0.08:
#                 return 'Positive'
#             elif score['compound'] <= -0.02:
#                 return 'Negative'
#             else:
#                 return 'Neutral'
#         except Exception as e:
#             # Log error if sentiment analysis fails for a specific text
#             # In a real scenario, you might log this to a separate table/service
#             dbt.log(f"Error processing sentiment for text: '{text[:80]}...' - {e}")
#             return None # Return None if analysis fails

#     # Apply sentiment analysis to the 'review_text' column
#     df['sentiment'] = df['review_text'].apply(get_sentiment_label)

#     # Select, rename, and order columns to match the dim_reviewer schema
#     # Ensure all columns from your dim_reviewer schema are present
#     dim_reviewer_df = df[[
#         'reviewer_id',
#         'reviewer_name',
#         'review_text',
#         'sentiment', # The newly generated sentiment
#         'review_timestamp' # This will be renamed to review_date
#     ]].copy() # Use .copy() to avoid SettingWithCopyWarning if you modify dim_reviewer_df further

#     # Rename review_timestamp to review_date as per your dim table schema
#     dim_reviewer_df = dim_reviewer_df.rename(columns={'review_timestamp': 'review_date'})

#     # Handle nulls for review_date if desired.
#     # Currently, it passes through whatever comes from review_timestamp.
#     # If you want to fill with a default date, uncomment and adjust:
#     # dim_reviewer_df['review_date'] = dim_reviewer_df['review_date'].fillna(pd.Timestamp('1900-01-01'))

#     # Ensure correct data types before returning for dbt to load into Postgres
#     # Pandas infers types, but explicit conversion can prevent issues
#     dim_reviewer_df['reviewer_id'] = dim_reviewer_df['reviewer_id'].astype(str)
#     dim_reviewer_df['reviewer_name'] = dim_reviewer_df['reviewer_name'].astype(str)
#     dim_reviewer_df['review_text'] = dim_reviewer_df['review_text'].astype(str)
#     # Convert sentiment to string (or category type if preferred)
#     dim_reviewer_df['sentiment'] = dim_reviewer_df['sentiment'].astype(str).replace('nan', None) # Handle None from previous step
#     # Ensure review_date is a datetime object or date string that Postgres understands
#     dim_reviewer_df['review_date'] = pd.to_datetime(dim_reviewer_df['review_date']).dt.date # Convert to date object

#     return dim_reviewer_df