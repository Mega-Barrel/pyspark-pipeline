
import time
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

        orders = job.fact_orders(orders_df=orders_df)
        events = job.fact_events(pages_df=pages_df, tracks_df=tracks_df)
        user_stats = job.build_user_stats(
            fact_events_df = events,
            fact_orders_df = orders
        )
        orders.show(n=10, truncate=False)
        print()
        events.show(n=10, truncate=False)
        print()
        user_stats.show(n=10, truncate=False)

    finally:
        time.sleep(10)
        job.stop()
