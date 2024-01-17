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

dim_location = spark.read.format("delta").load(f's3://{str_bucket_refined}/ecommerce/dw_dim_location')
fact_orders = spark.read.format("delta").load(f's3://{str_bucket_refined}/ecommerce/dw_fact_orders')

dim_location.createOrReplaceTempView("dim_location")
fact_orders.createOrReplaceTempView("fact_orders")

aggregate_sales_performance_by_city = spark.sql(
"""
SELECT
    d_loc.customer_state,
    d_loc.customer_city,
    COUNT(DISTINCT f_ord.order_id) as nu_qty_sales,
    ROUND((SUM(f_ord.nu_items_price) / COUNT(DISTINCT f_ord.order_id)),2) as nu_avg_ticket,
    ROUND(SUM(f_ord.nu_items_price),2) as nu_sum_items_price,
    COUNT(DISTINCT sk_customers) as nu_qty_unique_customers,
    ROUND(AVG(nu_freight_value),2) as nu_avg_freight_value,
    MAX(ts_order_purchase) as ts_last_order_purchase
FROM dim_location d_loc
LEFT JOIN fact_orders f_ord
    ON d_loc.customer_zip_code_prefix = f_ord.customer_zip_code_prefix
WHERE
    f_ord.order_status = "delivered"
GROUP BY 
    d_loc.customer_state,
    d_loc.customer_city
ORDER BY 
    nu_sum_items_price DESC
"""
)

str_path_aggregate_sales_performance_by_city = f's3://{str_bucket_refined}/ecommerce/aggregate_sales_performance_by_city'

aggregate_sales_performance_by_city.write \
    .format('delta') \
    .mode('overwrite') \
    .save(str_path_aggregate_sales_performance_by_city)

print("aggregate_sales_performance_by_city processed")


