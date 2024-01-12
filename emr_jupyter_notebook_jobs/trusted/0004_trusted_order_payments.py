from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import lit, col
from datetime import datetime, timedelta
import re

spark = SparkSession.builder \
    .appName('0004_trusted_order_payments') \
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

key_file_path = "ecommerce/olist_order_payments_dataset"

str_s3_raw_file_path = f's3://{str_bucket_raw}/{key_file_path}'
print(str_s3_raw_file_path)

raw_order_payments = spark.read.format("delta").load(str_s3_raw_file_path)

raw_order_payments.createOrReplaceTempView("raw_order_payments")

str_s3_trusted_file_path = f's3://{str_bucket_trusted}/{key_file_path}'
print(str_s3_trusted_file_path)

qtd_lines_operation = spark.sql(
f"""
        SELECT
            int(count(*)) as count
        FROM raw_order_payments as raw
        WHERE 
            ref_day_partition = {__REFDAY__}
            and raw.order_id NOT IN
            (
                SELECT DISTINCT order_id FROM delta.`s3://ecommerce-project-trusted/ecommerce/olist_order_payments_dataset/`
            )
"""
)

qtd_lines = qtd_lines_operation.collect()[0][0]
print("inserted rows: ", qtd_lines)

spark.sql(
f"""
    INSERT INTO delta.`s3://ecommerce-project-trusted/ecommerce/olist_order_payments_dataset/`
    (
        SELECT
            ref_day,
            ref_file_extraction,
            order_id,
            int(payment_sequential) as nu_payment_sequential,
            payment_type,
            int(payment_installments) as nu_payment_installments,
            cast(payment_value as float) as nu_payment_value
        FROM raw_order_payments as raw
        WHERE 
            ref_day_partition = {__REFDAY__}
            and raw.order_id NOT IN
            (
                SELECT DISTINCT order_id FROM delta.`s3://ecommerce-project-trusted/ecommerce/olist_order_payments_dataset/`
            )
    )
"""
)

print('inserted new orders data from trusted')

control = spark.sql(
    f"""
        SELECT
            "{str_bucket_raw}" as str_origin_zone,
            "{str_bucket_trusted}" as str_target_zone,
            '0004_trusted_order_payments' as str_process, 
            "{str_s3_raw_file_path}" as str_origin_file_path,
            "{str_s3_trusted_file_path}" as str_target_file_path,
            int("{str_proc_timestamp}") as dt_proc,
            null as num_affected_rows,
            null as num_updated_rows,
            null as num_deleted_rows,
            cast({qtd_lines} as long) as num_inserted_rows
    """
)

str_control_path = f's3://{str_bucket_control}/tb_0001_control_process_trusted'
print(str_control_path)

control.write \
    .format('delta') \
    .mode('append') \
    .save(str_control_path)

print('Log appended to control')
