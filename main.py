
import time

import pandas as pd
from ecommerce_pipeline.job.pipeline import PySparkJob
from ecommerce_pipeline.base.gcs_utils import GcsStorageManager

if __name__ == "__main__":

    BUCKET_NAME = "ecommerce_pipeline"
    CREDENTIALS_FILE = "credentials.json"
    LOCAL_CSV_FILE = "data/identifies.csv"
    CSV_NAME = "test-data/sample.csv"

    manager = GcsStorageManager(credentials_path=CREDENTIALS_FILE)
    manager.create_bucket(BUCKET_NAME)
    manager.upload_file(BUCKET_NAME, str(LOCAL_CSV_FILE), CSV_NAME)

    # identifiers_df = manager.read_csv_from_blob(BUCKET_NAME, CSV_NAME, pd=pd)
    # if identifiers_df is not None:
    #     print("\nDataFrame Head:")
    #     print(identifiers_df.head())

    job = PySparkJob()
    try:
        print("<<Reading Data>>")
        identifiers_df = job.read_csv(input_path="data/identifies.csv")   # or "data/identifiers.csv"
        tracks_df      = job.read_csv(input_path="data/tracks.csv")
        orders_df      = job.read_csv(input_path="data/order_completed.csv")
        pages_df       = job.read_csv(input_path="data/pages.csv")

        print("<<Transformation>>")
        df = job.transform(
            pages_df = pages_df,
            tracks_df = tracks_df,
            orders_df = orders_df
            # write = True
        )

        print("<<Load Data>>")
        df.show(n=10, truncate=False)

    finally:
        time.sleep(10)
        job.stop()
