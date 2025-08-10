
from ecommerce_pipeline.job.pipeline import PySparkJob

if __name__ == "__main__":

    job = PySparkJob()
    try:
        print("<<Reading Data>>")
        identifiers_df = job.read_csv(input_path="data/identifies.csv")   # or "data/identifiers.csv"
        tracks_df      = job.read_csv(input_path="data/tracks.csv")
        orders_df      = job.read_csv(input_path="data/order_completed.csv")
        pages_df       = job.read_csv(input_path="data/pages.csv")

        print("<<Transformation>>")

        dim_user_df = job.dim_users(
            identifiers_df,
            tracks_df,
            orders_df
        )
        dim_user_df[0].show(10, truncate=False)
        print()
        dim_user_df[1].show(10, truncate=False)

    finally:
        job.stop()
