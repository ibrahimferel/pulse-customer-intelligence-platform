import os
import zipfile
import pandas as pd
import gdown

from datetime import datetime

DRIVE_FILE_ID = "1BRrvsIDOk9soBlsKwWqMsS79oVBQgZtf"

SELECTED_DATASETS = [
    "orders.csv",
    "order_items.csv",
    "sellers.csv",
    "customers.csv",
    "geolocation.csv",
    "order_reviews.csv",
    "order_payments.csv"
]


def download_drive_file(
    file_id,
    output_path
):

    url = (
        f"https://drive.google.com/uc?id={file_id}"
    )

    gdown.download(
        url,
        output_path,
        quiet=False,
        fuzzy=True
    )


def fetch_dustiniadelixia_data():

    print(
        "Downloading DustiniaDelixia dataset..."
    )

    # LOCAL TESTING
    raw_dir = "data_lake/raw"

    # AIRFLOW VERSION NANTI:
    # raw_dir = "/opt/airflow/data_lake/raw"

    os.makedirs(
        raw_dir,
        exist_ok=True
    )

    zip_path = (
        f"{raw_dir}/dataset.zip"
    )

    try:

        # ---------- DOWNLOAD ----------

        download_drive_file(
            DRIVE_FILE_ID,
            zip_path
        )

        print(
            "\nDownload success."
        )

        print(
            f"ZIP size: "
            f"{os.path.getsize(zip_path)/1024/1024:.2f} MB"
        )

        # ---------- EXTRACT ----------

        with zipfile.ZipFile(
            zip_path,
            "r"
        ) as z:

            files = z.namelist()

            print(
                "\nAvailable files:"
            )

            print(files)

            for file_name in SELECTED_DATASETS:
                if file_name not in files:
                    print(
                        f"{file_name} not found."
                    )
                    continue

                print(
                    f"\nProcessing "
                    f"{file_name}"
                )

                df = pd.read_csv(
                    z.open(file_name)
                )

                dataset_name = (
                    file_name
                    .replace(
                        ".csv",
                        ""
                    )
                )

                current_time = (
                    datetime.now()
                    .strftime(
                        "%Y%m%d_%H%M%S"
                    )
                )

                dataset_dir = (
                    f"{raw_dir}/"
                    f"{dataset_name}"
                )

                os.makedirs(
                    dataset_dir,
                    exist_ok=True
                )

                output_path = (
                    f"{dataset_dir}/"
                    f"{dataset_name}_"
                    f"{current_time}.parquet"
                )

                df.to_parquet(
                    output_path,
                    index=False
                )

                print(
                    f"Saved {len(df)} rows"
                )

                print(
                    f"Shape: {df.shape}"
                )

                print(
                    output_path
                )
        print(
            "\nFetch completed."
        )

    except Exception as e:
        print(
            f"\nFetch failed: {e}"
        )
        raise


if __name__ == "__main__":
    fetch_dustiniadelixia_data()