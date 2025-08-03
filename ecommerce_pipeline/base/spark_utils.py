
from typing import Optional, Dict
from pyspark.sql import SparkSession, DataFrame

class PySparkJobInterface():
    """An interface for managing PySpark jobs and sessions.

    This class provides a standardized way to initialize a SparkSession,
    read data, perform basic inspections, and properly terminate the session.
    It is designed to be inherited by specific job implementations that
    require a Spark context.

    Attributes:
        spark (SparkSession): The active SparkSession managed by this instance.
    """

    def __init__(self, app_name: str = "EcommerPipeline", configs: Optional[Dict[str, str]] = None):
        """Initializes the PySparkJobInterface and creates a SparkSession.

        Args:
            app_name (str, optional): The name for the Spark application.
                Defaults to "EcommerPipeline".
            configs (Optional[Dict[str, str]], optional): A dictionary of
                Spark configuration options to apply to the session.
                Defaults to None.
        """
        self.spark = self._create_spark_session(app_name=app_name, configs=configs)

    def _create_spark_session(self, app_name: str, configs: Optional[Dict[str, str]] = None) -> SparkSession:
        """Creates or retrieves a SparkSession with the specified configurations.

        This method initializes a SparkSession.Builder, applies any custom
        configurations provided, and then gets or creates the session.

        Args:
            app_name (str): The name of the Spark application.
            configs (Optional[Dict[str, str]], optional): A dictionary of
                Spark configuration key-value pairs. Defaults to None.

        Returns:
            SparkSession: The configured SparkSession object.
        """
        builder = SparkSession.builder.appName(app_name)
        if configs:
            for key, value in configs.items():
                builder = builder.config(key, value)
        spark = builder.getOrCreate()
        return spark

    # pylint: disable=E1101
    def read_csv(self, input_path: str) -> DataFrame:
        """ Read CSV file """
        return self.spark.read.options(header=True, inferSchema=True).csv(input_path)

    def show_schema(self, df: DataFrame) -> None:
        """Prints the schema of a DataFrame to the console.

        Args:
            df (DataFrame): The DataFrame whose schema will be printed.
        
        Returns:
            None
        """
        df.printSchema()

    def show_data(self, df: DataFrame, n: int = 10) -> None:
        """Displays the first 'n' rows of a DataFrame.

        Args:
            df (DataFrame): The DataFrame to display.
            n (int, optional): The number of rows to show. Defaults to 10.
        
        Returns:
            None
        """
        df.show(n=n, truncate=False)

    def stop(self) -> None:
        """Stops the active SparkSession.

        It's important to call this method at the end of a job to release
        resources.
        
        Returns:
            None
        """
        self.spark.stop()
