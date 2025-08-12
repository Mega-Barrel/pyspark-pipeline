# ecommerce_pipeline/job/pipeline.py
from pathlib import Path
from typing import Dict
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from ecommerce_pipeline.base.spark_utils import PySparkJobInterface

OUT_DIR = Path(__file__).resolve().parents[2] / "output" / "parquet"


class PySparkJob(PySparkJobInterface):
    """
    Build star schema (facts + dims) and a user_stats gold table from RudderStack-like CSVs.

    Use from main.py:
        job = PySparkJob()
        outputs = job.transform(identifiers_df, pages_df, tracks_df, orders_df, write=True)
    """

    def __init__(self):
        super().__init__(app_name="EcommercePipeline", configs={
            "spark.sql.session.timeZone": "UTC",
            "spark.sql.sources.partitionOverwriteMode": "dynamic",
        })

    # ---------------------------------------------------------------------
    # Helpers
    # ---------------------------------------------------------------------
    def _with_user_key(self, df: DataFrame) -> DataFrame:
        """Stable user_key hash from (user_id, email, anonymous_id)."""
        return df.withColumn(
            "user_key",
            F.concat_ws(
                "||",
                F.coalesce(F.col("user_id"), F.lit("")),
                F.lower(F.coalesce(F.col("email"), F.lit("")))
            )
        )

    def _parse_ts(self, df: DataFrame, src_col: str, dst_col: str) -> DataFrame:
        """Parse 'yyyy-MM-dd HH:mm:ss' timestamps into TimestampType."""
        return df.withColumn(dst_col, F.to_timestamp(F.col(src_col), "yyyy-MM-dd HH:mm:ss"))

    def _prepare_inputs(
        self,
        pages_df: DataFrame,
        tracks_df: DataFrame,
        orders_df: DataFrame
    ):
        """Type timestamps & cast numeric fields once."""
        pages = self._parse_ts(pages_df, "timestamp", "timestamp")
        tracks = self._parse_ts(tracks_df, "timestamp", "timestamp")
        orders = (
            self._parse_ts(orders_df, "timestamp", "timestamp")
            .withColumn("order_value", F.col("order_value").cast("double"))
            .withColumn("items_purchased", F.col("items_purchased").cast("int"))
        )
        return pages, tracks, orders

    # ---------------------------------------------------------------------
    # Facts
    # ---------------------------------------------------------------------
    def fact_orders(self, orders_df: DataFrame) -> DataFrame:
        """
        One row per order with user_key, session_id and order_date.
        """
        return (
            self._with_user_key(orders_df)
            .select(
                "order_id",
                "timestamp",
                F.to_date("timestamp").alias("order_date"),
                F.col("context_session_id").alias("session_id"),
                "user_id",
                "email",
                "anonymous_id",
                "user_key",
                "order_value",
                "items_purchased",
            )
        )

    def fact_events(self, pages_df: DataFrame, tracks_df: DataFrame) -> DataFrame:
        """
        Unify page views and track events into a single events fact with session_id and event_date.
        """
        pages_e = (
            self._with_user_key(pages_df)
            .select(
                "anonymous_id",
                "user_id",
                "email",
                F.col("context_session_id").alias("session_id"),
                "timestamp",
                F.col("page_url").alias("url"),
            )
            .withColumn("event_name", F.lit("page_view"))
        )

        tracks_e = (
            self._with_user_key(tracks_df)
            .select(
                "anonymous_id",
                "user_id",
                "email",
                F.col("context_session_id").alias("session_id"),
                "timestamp",
                "event_name",
            )
            .withColumn("url", F.lit(None).cast("string"))
        )

        events = pages_e.unionByName(tracks_e, allowMissingColumns=True)
        return events.withColumn("event_date", F.to_date("timestamp"))

    # ---------------------------------------------------------------------
    # Fact: User Stats Table
    # ---------------------------------------------------------------------
    def build_user_stats(self, fact_events_df: DataFrame, fact_orders_df: DataFrame) -> DataFrame:
        """
        Per-user RFM-style metrics snapshot: sessions/events/orders/revenue + recency KPIs.
        """
        ev = self._with_user_key(fact_events_df)
        ordf = fact_orders_df

        per_user_events = (
            ev.groupBy("user_key")
            .agg(
                F.min("timestamp").alias("event_first_timestamp"),
                F.max("timestamp").alias("event_last_timestamp"),
                F.countDistinct("session_id").alias("total_sessions"),
                F.count(F.lit(1)).alias("total_events"),
            )
        )

        per_user_orders = (
            ordf.groupBy("user_key")
            .agg(
                F.min("timestamp").alias("order_first_timestamp"),
                F.max("timestamp").alias("order_last_timestamp"),
                F.sum("order_value").alias("total_revenue"),
                F.countDistinct("order_id").alias("total_orders"),
            )
        )

        return (
            per_user_events.join(per_user_orders, "user_key", "left")
            .withColumn(
                "aov",
                F.when(F.col("total_orders") > 0, F.col("total_revenue") / F.col("total_orders"))
                 .otherwise(F.lit(None).cast("double")),
            )
            .withColumn(
                "conversion_rate",
                F.when(F.col("total_sessions") > 0, F.col("total_orders") / F.col("total_sessions"))
                 .otherwise(F.lit(0.0)),
            )
            .withColumn("days_since_last_event", F.datediff(F.current_date(), F.to_date("event_last_timestamp")))
            .withColumn("days_since_last_order", F.datediff(F.current_date(), F.to_date("order_last_timestamp")))
            .withColumn("snapshot_date", F.current_date())
        )

    # ---------------------------------------------------------------------
    # Orchestration
    # ---------------------------------------------------------------------
    def transform(
        self,
        pages_df: DataFrame,
        tracks_df: DataFrame,
        orders_df: DataFrame,
        # write: bool = True
    ) -> Dict[str, DataFrame]:
        """
        Orchestrates: build facts, dims, gold. Optionally write Parquet.
        Returns DataFrames for interactive previews/tests.
        """
        pages, tracks, orders = self._prepare_inputs(pages_df, tracks_df, orders_df)

        fact_events = self.fact_events(pages, tracks)
        fact_orders = self.fact_orders(orders)
        user_stats = self.build_user_stats(fact_events, fact_orders)

        # if write:
            # self._write_parquet(user_stats, "user_stats", partition_cols=["snapshot_date"])
            # self._write_csv_pandas(user_stats, "user_stats_csv")

        return user_stats

    # # Optional: keep a run() for ad-hoc/manual runs if you prefer class-driven execution.
    # def run(self) -> None:
    #     """
    #     Optional: implement if you want this class to read CSVs itself.
    #     Your current main.py already reads DataFrames and calls transform(), so this is unused.
    #     """
    #     raise NotImplementedError("Use main.py to read CSVs and call job.transform(...).")
