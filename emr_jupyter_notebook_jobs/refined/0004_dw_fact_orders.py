from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import lit, col
from datetime import datetime, timedelta
import re

spark = SparkSession.builder \
    .appName('0004_dw_fact_orders') \
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

str_path_tb_orders = f's3://{str_bucket_trusted}/ecommerce/olist_orders_dataset'
str_path_tb_order_items = f's3://{str_bucket_trusted}/ecommerce/olist_order_items_dataset'
str_path_tb_order_payments = f's3://{str_bucket_trusted}/ecommerce/olist_order_payments_dataset'
str_path_dim_customers = f's3://{str_bucket_refined}/ecommerce/dw_dim_customers'

tb_orders = spark.read.format('delta').load(str_path_tb_orders)
tb_order_items = spark.read.format('delta').load(str_path_tb_order_items)
tb_order_payments = spark.read.format('delta').load(str_path_tb_order_payments)
dim_customers = spark.read.format('delta').load(str_path_dim_customers)

tb_orders.createOrReplaceTempView('tb_orders')
tb_order_items.createOrReplaceTempView('tb_order_items')
tb_order_payments.createOrReplaceTempView('tb_order_payments')
dim_customers.createOrReplaceTempView('dim_customers')

tb_fact_orders = spark.sql(
"""
    SELECT 
        o.order_id,
        c.sk_customers,
        c.customer_zip_code_prefix,
        o.order_status,
        o.ts_order_purchase,
        o.ts_order_approved_at,
        o.ts_order_delivered_carrier,
        o.ts_order_delivered_customer,
        o.dt_order_estimated_delivery,
        i.product_id,
        i.nu_price as nu_items_price,
        i.nu_freight_value,
        p.payment_type,
        p.nu_payment_installments,
        p.nu_payment_value
    FROM tb_orders as o
    LEFT JOIN dim_customers as c ON o.customer_id = c.customer_id AND
        CASE
            WHEN c.ts_end_date IS NOT NULL THEN o.ts_order_purchase BETWEEN c.ts_start_date AND c.ts_end_date
            WHEN c.ts_end_date IS NULL THEN o.ts_order_purchase >= c.ts_start_date
        END
    LEFT JOIN tb_order_items as i ON o.order_id = i.order_id
    LEFT JOIN tb_order_payments  as p ON o.order_id = p.order_id
"""
)

str_path_dw_fact_orders = f's3://{str_bucket_refined}/ecommerce/dw_fact_orders'

tb_fact_orders.write \
    .format('delta') \
    .mode('overwrite') \
    .save(str_path_dw_fact_orders)

print("orders fact processed")
