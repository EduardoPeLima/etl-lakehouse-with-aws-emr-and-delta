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

__REFDAY__ = 20240110

key_file_path = "ecommerce/olist_products_dataset"

str_s3_raw_file_path = f's3://{str_bucket_raw}/{key_file_path}'
print(str_s3_raw_file_path)

raw_products = spark.read.format("delta").load(str_s3_raw_file_path)

raw_products.createOrReplaceTempView("raw_products")

str_s3_trusted_file_path = f's3://{str_bucket_trusted}/{key_file_path}'
print(str_s3_trusted_file_path)

merge = spark.sql(
f"""
MERGE INTO delta.`s3://ecommerce-project-trusted/ecommerce/olist_products_dataset/` AS target
USING (
    SELECT
        ref_day,
        ref_file_extraction,
        product_id,
        product_category_name,
        int(product_name_lenght) as nu_product_name_lenght,
        int(product_description_lenght) as nu_product_description_lenght,
        int(product_photos_qty) as nu_product_photos_qty,
        cast(product_weight_g as float) as nu_product_weight_g,
        cast(product_length_cm as float) as nu_product_length_cm,
        cast(product_height_cm as float) as nu_product_height_cm,
        cast(product_width_cm as float) as nu_product_width_cm
    FROM raw_products as raw
    WHERE 
        ref_day_partition = '{__REFDAY__}'
) AS source
ON target.product_id = source.product_id
WHEN NOT MATCHED THEN
    INSERT (
        ref_day,
        ref_file_extraction,
        product_id,
        product_category_name,
        nu_product_name_lenght,
        nu_product_description_lenght,
        nu_product_photos_qty,
        nu_product_weight_g,
        nu_product_length_cm,
        nu_product_height_cm,
        nu_product_width_cm
    ) VALUES (
        source.ref_day,
        source.ref_file_extraction,
        source.product_id,
        source.product_category_name,
        source.nu_product_name_lenght,
        source.nu_product_description_lenght,
        source.nu_product_photos_qty,
        source.nu_product_weight_g,
        source.nu_product_length_cm,
        source.nu_product_height_cm,
        source.nu_product_width_cm
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
            '0005_trusted_products' as str_process, 
            "{str_s3_raw_file_path}" as str_origin_file_path,
            "{str_s3_trusted_file_path}" as str_target_file_path,
            int("{str_proc_timestamp}") as dt_proc,
            null as num_affected_rows,
            null as num_updated_rows,
            null as num_deleted_rows,
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


