
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count
from pyspark.sql.types import IntegerType, FloatType, StringType, StructType, StructField

class PySparkJob:

    def init_spark_session(self) -> SparkSession:
        return SparkSession.builder.appName("Ecommerce Data Pipeline").getOrCreate()
