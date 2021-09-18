from pyspark.sql import SparkSession
from job_Pensionistas import jobPensionistas

if __name__ == "__main__":
    spark = (
        SparkSession
        .builder
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )
    job = jobPensionistas(spark)
    job.run()
    spark.stop()