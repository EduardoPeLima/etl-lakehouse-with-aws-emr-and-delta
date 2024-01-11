from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import lit, col
from datetime import datetime, timedelta
import re

spark = SparkSession.builder \
    .appName('0002_trusted_orders') \
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

#prefix list

#dt: datetime
#str: string

#cache and count are commented because they were used just to make the tests faster, saving cache and activating a Spark action.

str_bucket_raw = "ecommerce-project-raw"
str_bucket_trusted = "ecommerce-project-trusted"
str_bucket_control = "ecommerce-project-control"

ts_proc = datetime.now()
str_proc_timestamp = ts_proc.strftime("%Y%m%d%H%M%S")
__REFDAY__ = int(ts_proc.strftime("%Y%m%d"))

key_file_path = "ecommerce/olist_orders_dataset"

str_s3_raw_file_path = f's3://{str_bucket_raw}/{key_file_path}'
print(str_s3_raw_file_path)

raw_orders = spark.read.format("delta").load(str_s3_raw_file_path)

raw_orders.createOrReplaceTempView("raw_orders")

str_s3_trusted_file_path = f's3://{str_bucket_trusted}/{key_file_path}'
print(str_s3_trusted_file_path)

merge = spark.sql(
f"""
    MERGE INTO delta.`s3://ecommerce-project-trusted/ecommerce/olist_orders_dataset/` as trusted
    USING (
        SELECT 
            ref_day,
            ref_file_extraction,
            order_id,
            customer_id,
            order_status,
            ts_order_purchase,
            ts_order_approved_at,
            ts_order_delivered_carrier,
            ts_order_delivered_customer,
            dt_order_estimated_delivery
        FROM 
        (
            SELECT
                ref_day,
                ref_file_extraction,
                order_id,
                customer_id,
                order_status,
                cast(order_purchase_timestamp as timestamp) as ts_order_purchase,
                cast(order_approved_at as timestamp) as ts_order_approved_at,
                cast(order_delivered_carrier_date as timestamp) as ts_order_delivered_carrier,
                cast(order_delivered_customer_date as timestamp) as ts_order_delivered_customer,
                cast(order_estimated_delivery_date as date) as dt_order_estimated_delivery,
                ROW_NUMBER() OVER (PARTITION BY raw.order_id ORDER BY raw.ref_file_extraction DESC) as row_num
            FROM raw_orders as raw  
            WHERE ref_day_partition = {__REFDAY__}
        )
        WHERE row_num = 1
    ) as sub
    ON trusted.order_id = sub.order_id
    WHEN MATCHED AND trusted.ref_file_extraction < sub.ref_file_extraction THEN
        UPDATE SET
            order_status = sub.order_status,
            ts_order_purchase = sub.ts_order_purchase,
            ts_order_approved_at = sub.ts_order_approved_at,
            ts_order_delivered_carrier = sub.ts_order_delivered_carrier,
            ts_order_delivered_customer = sub.ts_order_delivered_customer,
            dt_order_estimated_delivery = sub.dt_order_estimated_delivery
    WHEN NOT MATCHED 
        THEN INSERT (
            ref_day,
            ref_file_extraction,
            order_id,
            customer_id,
            order_status,
            ts_order_purchase,
            ts_order_approved_at,
            ts_order_delivered_carrier,
            ts_order_delivered_customer,
            dt_order_estimated_delivery
        )
        VALUES (
            sub.ref_day,
            sub.ref_file_extraction,
            sub.order_id,
            sub.customer_id,
            sub.order_status,
            sub.ts_order_purchase,
            sub.ts_order_approved_at,
            sub.ts_order_delivered_carrier,
            sub.ts_order_delivered_customer,
            sub.dt_order_estimated_delivery
        )
        
"""
)

merge.createOrReplaceTempView("merge")

print("merge completed from raw to trusted")
print(merge.show())

control = spark.sql(
    f"""
        SELECT
            "{str_bucket_raw}" as str_origin_zone,
            "{str_bucket_trusted}" as str_target_zone,
            '0002_trusted_orders' as str_process, 
            "{str_s3_raw_file_path}" as str_origin_file_path,
            "{str_s3_trusted_file_path}" as str_target_file_path,
            int("{str_proc_timestamp}") as dt_proc,
            num_affected_rows,
            num_updated_rows,
            num_deleted_rows,
            num_inserted_rows
        FROM merge
    """
)

str_control_path = f's3://{str_bucket_control}/tb_0001_control_process_trusted'
print(str_control_path)

control.write \
    .format('delta') \
    .mode('append') \
    .save(str_control_path)

print('Log appended to control')
