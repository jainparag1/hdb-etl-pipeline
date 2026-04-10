# src/pipeline.py

from extract import download_data
from profiling import load_and_combine, profile_data
from validation import validate
from transform import generate_identifier
from hashing import hash_identifier

def save_outputs(df_clean, df_transformed, df_failed, df_hashed):
    for d in [df_clean, df_transformed, df_failed, df_hashed]:
        if 'remaining_lease' in d.columns:
            d['remaining_lease'] = d['remaining_lease'].astype('string')
    df_clean.to_parquet("data/cleaned/cleaned.parquet")
    df_transformed.to_parquet("data/transformed/transformed.parquet")
    df_failed.to_parquet("data/failed/failed.parquet")
    df_hashed.to_parquet("data/hashed/hashed.parquet")
    print("Outputs saved successfully")

def deduplicate(df):
    key_cols = [col for col in df.columns if col != 'resale_price']
    df_sorted = df.sort_values(by='resale_price', ascending=False)
    deduped = df_sorted.drop_duplicates(subset=key_cols, keep='first')
    failed = df[~df.index.isin(deduped.index)]
    return deduped, failed

def run_pipeline():
    # df = download_data()

    df = load_and_combine()

    profile = profile_data(df)
    # print(f"Profile: {profile}")

    df_valid, df_failed = validate(df)
    print(f"Valid: {df_valid.shape}")
    print(f"Failed: {df_failed.shape}")


    df_dedup, df_dup_failed = deduplicate(df_valid)
    print(f"df_dedup: {df_dedup.shape}")
    print(f"df_dup_failed: {df_dup_failed.shape}")

    df_transformed = generate_identifier(df_dedup)
    print(f"df_transformed: {df_transformed.shape}")

    df_hashed = hash_identifier(df_transformed)
    print(f"df_hashed: {df_hashed.shape}")
    print(f"first two df_hashed records : {df_hashed.head(2)}")

    save_outputs(df_dedup, df_transformed, df_failed, df_hashed)

if __name__ == "__main__":
    run_pipeline()