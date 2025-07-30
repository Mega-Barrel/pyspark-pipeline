
import abc
from pyspark.sql import SparkSession, DataFrame

class PySparkJobInterface(abc.ABC):
    """ Abstract Class"""

    def __init__(self):
        self.spark = self.init_spark_session()

    @abc.abstractmethod
    def init_spark_session(self) -> SparkSession:
        """Create Spark Session"""
        return NotImplementedError

    # pylint: disable=E1101
    def read_csv(self, input_path: str) -> DataFrame:
        """ Read CSV file """
        return self.spark.read.options(header=True, inferSchema=True).csv(input_path)

    def stop(self) -> None:
        """ Terminate Spark Session """
        self.spark.stop()
