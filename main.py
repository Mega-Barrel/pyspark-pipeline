
import time
from ecommerce_pipeline.job.pipeline import PySparkJob

if __name__ == "__main__":

    job = PySparkJob()
    try:
        # print(job.get_spark_version())
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
