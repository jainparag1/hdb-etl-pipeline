from pathlib import Path
import requests
import pandas as pd
import time

BASE_DIR = Path(".").resolve()
RAW_DIR = BASE_DIR / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

s = requests.Session()

DATASETS = [
    {
        "name": "ResaleFlatPricesBasedonApprovalDate19901999",
        "dataset_id": "d_ebc5ab87086db484f88045b47411ebc5",
    },
    {
        "name": "ResaleFlatPricesBasedonApprovalDate2000Feb2012",
        "dataset_id": "d_43f493c6c50d54243cc1eab0df142d6a",
    },
    {
        "name": "ResaleFlatPricesBasedonRegistrationDateFromMar2012toDec2014",
        "dataset_id": "d_2d5ff9ea31397b66239f245f57751537",
    },
    {
        "name": "ResaleFlatPricesBasedonRegistrationDateFromJan2015toDec2016",
        "dataset_id": "d_ea9ed51da2787afaf8e51f827c304208",
    },
    {
        "name": "ResaleflatpricesbasedonregistrationdatefromJan2017onwards",
        "dataset_id": "d_8b84c4ee58e3cfc0ece0d773c8ca6abc",
    },
]


def download_file(dataset_id, output_file):
    headers = {"Content-Type": "application/json"}

    init = s.get(
        f"https://api-open.data.gov.sg/v1/public/api/datasets/{dataset_id}/initiate-download",
        headers=headers,
        json={}
    )
    if init.status_code == 429:
        raise RuntimeError("Rate-limited on initiate-download; wait and retry")
    init.raise_for_status()

    max_polls = 8
    for i in range(max_polls):
        poll = s.get(
            f"https://api-open.data.gov.sg/v1/public/api/datasets/{dataset_id}/poll-download",
            headers=headers,
            json={}
        )
        if poll.status_code == 429:
            time.sleep(15)
            continue
        poll.raise_for_status()

        payload = poll.json()
        data = payload.get("data") or {}
        status = data.get("status")
        url = data.get("url")

        if status == "DOWNLOAD_SUCCESS" and url:
            df = pd.read_csv(url)
            print(df.head())   # instead of display(...)
            df.to_csv(output_file, index=False)
            print("Dataframe loaded and saved.")
            return

        print(f"{i+1}/{max_polls}: not ready yet ({status}), polling again...")
        time.sleep(4)

    raise RuntimeError(f"Download URL not ready for {dataset_id}")


def download_data():
    downloaded_files = []

    for dataset in DATASETS:
        # csv_url = get_csv_url(dataset["dataset_id"])
        DATASET_ID = dataset["dataset_id"]
        output_file = RAW_DIR / f"{dataset['name']}.csv"
            # download_csv(csv_url, output_file)
        #     download_file(DATASET_ID, output_file)
        #     downloaded_files.append(output_file)
        # except Exception as e:
        #     print(f"Skipping {dataset['name']}: {e}")
        max_attempts = 5
        for attempt in range(max_attempts):
            try:
                download_file(dataset["dataset_id"], output_file)
                downloaded_files.append(output_file)
                break
            except RuntimeError as e:
                if "Rate-limited" in str(e):
                    wait = 15 * (attempt + 1)  # 15s, 30s, 45s...
                    print(f"{dataset['name']}: rate-limited, waiting {wait}s before retry...")
                    time.sleep(wait)
                    continue
                raise

    print("Downloaded files:")
    for file in downloaded_files:
        print(file)

    for file in downloaded_files:
        df = pd.read_csv(file)
        print(f"{file.name}: shape={df.shape}, columns={list(df.columns)}")


if __name__ == "__main__":
    download_data()