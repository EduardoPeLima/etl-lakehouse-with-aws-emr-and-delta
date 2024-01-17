from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import lit, col
from datetime import datetime, timedelta
import re

spark = SparkSession.builder \
    .appName('0001_dw_dim_products') \
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

str_path_tb_products = f's3://{str_bucket_trusted}/ecommerce/olist_products_dataset'

tb_products = spark.read.format('delta').load(str_path_tb_products)

tb_products = tb_products.withColumn("ref_dw_process", lit(str_proc_timestamp))

str_path_dw_product = f's3://{str_bucket_refined}/ecommerce/dw_dim_products'

tb_products.write \
    .format('delta') \
    .mode('overwrite') \
    .save(str_path_dw_product)

print("products dimension processed")


