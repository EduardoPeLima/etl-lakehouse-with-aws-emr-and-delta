from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import lit, col
from datetime import datetime, timedelta
import re

spark = SparkSession.builder \
    .appName('0002_dw_dim_location') \
    .config("spark.jars.packages", \
            "io.delta:delta-core_2.12:2.4.0") \
    .config("spark.jars.packages", \
            "io.delta:delta-storage-2.4.0") \
    .config("spark.sql.extensions", \
            "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog",\
            "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.addPyFile("s3://spark-addons/"\
                             +"delta-core_2.12-2.4.0.jar")

str_bucket_trusted = "ecommerce-project-trusted"
str_bucket_refined = "ecommerce-project-refined"

ts_proc = datetime.now()
str_proc_timestamp = ts_proc.strftime("%Y%m%d%H%M%S")

#tables used for dimension

str_path_tb_customers = f's3://{str_bucket_trusted}/ecommerce/olist_customers_dataset'

tb_customers = spark.read.format('delta').load(str_path_tb_customers)

tb_customers.createOrReplaceTempView('tb_customers')

tb_location = spark.sql(
"""
    SELECT DISTINCT
        customer_zip_code_prefix,
        customer_city,
        customer_state
    FROM tb_customers
    WHERE customer_zip_code_prefix IS NOT NULL
"""
)

str_path_dw_location = f's3://{str_bucket_refined}/ecommerce/dw_dim_location'

tb_location.write \
    .format('delta') \
    .mode('overwrite') \
    .save(str_path_dw_location)

print("location dimension processed")
