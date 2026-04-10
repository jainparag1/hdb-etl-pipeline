# src/validation.py
import pandas as pd

def validate(df):
    failed = pd.DataFrame()
    
    # Example rules
    valid_df = df.copy()

    # Date format check
    valid_df = valid_df[valid_df['month'].str.match(r'\d{4}-\d{2}')]

    # Town non-null
    valid_df = valid_df[valid_df['town'].notnull()]

    # Flat type known values
    allowed_flat_types = df['flat_type'].dropna().unique()
    valid_df = valid_df[valid_df['flat_type'].isin(allowed_flat_types)]

    # Storey range format
    valid_df = valid_df[valid_df['storey_range'].str.contains(r'\d+')]

    # Split failed
    failed = df[~df.index.isin(valid_df.index)]

    return valid_df, failed

def deduplicate(df):
    failed = pd.DataFrame()
    key_cols = [col for col in df.columns if col != 'resale_price']

    df_sorted = df.sort_values(by='resale_price', ascending=False)

    deduped = df_sorted.drop_duplicates(subset=key_cols, keep='first')

    failed = df[~df.index.isin(deduped.index)]

    return deduped, failed