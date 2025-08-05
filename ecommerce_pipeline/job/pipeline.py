
from ecommerce_pipeline.base import PySparkJobInterface

class PySparkJob(PySparkJobInterface):

    def __init__(self):
        super().__init__()

    def extract_data(self):
        identifier_df = self.read_csv(input_path='data/identifies.csv')
        tracks_df = self.read_csv(input_path='data/tracks.csv')
        successfull_orders_df = self.read_csv(input_path='data/order_completed.csv')
        pages_df = self.read_csv(input_path='data/pages.csv')

        return (
            identifier_df,
            tracks_df,
            successfull_orders_df,
            pages_df
        )

    def run(self):
        print("Reading data from files..")
        data = self.extract_data()
        self.show_data(data[0])
