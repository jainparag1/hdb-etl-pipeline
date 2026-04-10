import pandas as pd

def deduplicate(df):
    key_cols = [col for col in df.columns if col != 'resale_price']

    df_sorted = df.sort_values(by='resale_price', ascending=False)

    deduped = df_sorted.drop_duplicates(subset=key_cols, keep='first')

    failed = df[~df.index.isin(deduped.index)]

    return deduped, failed