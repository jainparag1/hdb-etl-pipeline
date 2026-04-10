# src/profiling.py
# import pandas as pd
# import glob

# def load_and_combine(path="../data/raw/*.csv"):
#     df = pd.concat([pd.read_csv(f) for f in glob.glob(path)])
#     print(df.head())
#     print(df.shape)
#     print(df.columns)
#     print(df.dtypes)
#     print(df.isnull().sum())
#     print(df.nunique())
#     return df

# load_and_combine()

from pathlib import Path
import pandas as pd
import glob
def load_and_combine(path=None):
    if path is None:
        raw_dir = Path(__file__).resolve().parents[1] / "data" / "raw"
        path = str(raw_dir / "*.csv")
    files = glob.glob(path)
    if not files:
        raise FileNotFoundError(f"No CSV files found for pattern: {path}")
    df = pd.concat([pd.read_csv(f) for f in files], ignore_index=True)
    # print(df.head())
    # print(df.shape)
    # print(df.columns)
    # print(df.dtypes)
    # print(df.isnull().sum())
    # print(df.nunique())
    return df

def profile_data(df):
    profile = {
        "row_count": len(df),
        "columns": df.columns.tolist(),
        "missing": df.isnull().sum().to_dict(),
        "dtypes": df.dtypes.astype(str).to_dict(),
        "unique_counts": df.nunique().to_dict()
    }
    return profile