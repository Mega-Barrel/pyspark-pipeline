
from ecommerce_pipeline.job.pipeline import PySparkJob

if __name__ == "__main__":
    job = PySparkJob()
    try:
        job.run()
    finally:
        job.stop()