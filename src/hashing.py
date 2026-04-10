# src/hashing.py
import hashlib

def hash_identifier(df):
    df['hashed_id'] = df['resale_id'].apply(
        lambda x: hashlib.sha256(x.encode()).hexdigest()
    )
    return df