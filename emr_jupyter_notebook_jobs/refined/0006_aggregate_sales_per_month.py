from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import lit, col
from datetime import datetime, timedelta
import re

spark = SparkSession.builder \
    .appName('0005_trusted_products') \
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

str_bucket_refined = "ecommerce-project-refined"

ts_proc = datetime.now()
str_proc_timestamp = ts_proc.strftime("%Y%m%d%H%M%S")

fact_orders = spark.read.format("delta").load(f's3://{str_bucket_refined}/ecommerce/dw_fact_orders')

fact_orders.createOrReplaceTempView("fact_orders")

aggregate_sales_per_month = spark.sql(
"""
SELECT
    DATE(ts_order_purchase) as dt_order_purchase,
    payment_type,
    ROUND(SUM(nu_items_price),2) as nu_sum_items_value,
    COUNT(DISTINCT order_id) as nu_qty_orders
FROM fact_orders
WHERE 
    order_status = "delivered"
GROUP BY 
    dt_order_purchase, payment_type
ORDER BY dt_order_purchase, payment_type, nu_sum_items_value DESC
"""
)

str_path_aggregate_sales_per_month = f's3://{str_bucket_refined}/ecommerce/aggregate_sales_per_month'

aggregate_sales_per_month.write \
    .format('delta') \
    .mode('overwrite') \
    .save(str_path_aggregate_sales_per_month)

print("aggregate_sales_per_month processed")


