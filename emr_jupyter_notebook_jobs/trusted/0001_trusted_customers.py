from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import lit, col
from datetime import datetime, timedelta
import re

spark = SparkSession.builder \
    .appName('0001_raw_customers') \
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

key_file_path = "ecommerce/olist_customers_dataset"

str_s3_raw_file_path = f's3://{str_bucket_raw}/{key_file_path}'
print(str_s3_raw_file_path)

raw_customers = spark.read.format("delta").load(str_s3_raw_file_path)

raw_customers.createOrReplaceTempView("raw_customers")

#raw_customers.cache()
#qtd=raw_customers.count()
#print('rows from landzone file: ', qtd)

#SCD2 will be utilized, and must happen a new register for a customer when his 
#customer_zip_code_prefix, customer_city or customer_state has changed.

str_s3_trusted_file_path = f's3://{str_bucket_trusted}/{key_file_path}'
print(str_s3_trusted_file_path)

merge = spark.sql(
f"""
    MERGE INTO delta.`s3://ecommerce-project-trusted/ecommerce/olist_customers_dataset` as trusted
    USING
    (
        SELECT 
            *
        FROM 
        (
            SELECT
                raw.customer_id as join_key,
                raw.ref_day,
                raw.ref_file_extraction,
                raw.customer_id,
                raw.customer_unique_id,
                raw.customer_zip_code_prefix,
                raw.customer_city,
                raw.customer_state,
                ROW_NUMBER() OVER (PARTITION BY raw.customer_id ORDER BY raw.ref_file_extraction DESC) as row_num
            FROM raw_customers as raw
            INNER JOIN delta.`s3://ecommerce-project-trusted/ecommerce/olist_customers_dataset` as trusted
            ON raw.customer_id = trusted.customer_id
            WHERE 
                raw.ref_day_partition = {__REFDAY__}
                AND raw.ref_file_extraction > trusted.ref_file_extraction
        )
        WHERE row_num = 1
        
        UNION ALL
        
        SELECT
            *
        FROM
        (
            SELECT
                NULL as join_key,
                raw.ref_day,
                raw.ref_file_extraction,
                raw.customer_id,
                raw.customer_unique_id,
                raw.customer_zip_code_prefix,
                raw.customer_city,
                raw.customer_state,
                ROW_NUMBER() OVER (PARTITION BY raw.customer_id ORDER BY raw.ref_file_extraction DESC) as row_num
            FROM raw_customers as raw
            INNER JOIN delta.`s3://ecommerce-project-trusted/ecommerce/olist_customers_dataset` as trusted
            ON raw.customer_id = trusted.customer_id
            WHERE
                raw.ref_day_partition = {__REFDAY__}
                AND
                    (
                    raw.customer_zip_code_prefix <> trusted.customer_zip_code_prefix 
                    OR raw.customer_city <> trusted.customer_city
                    OR raw.customer_state <> trusted.customer_state
                    ) 
                AND trusted.flag_scd_active = True
                AND raw.ref_file_extraction > trusted.ref_file_extraction
        )
        WHERE row_num = 1
            
    ) as sub
    ON sub.join_key = trusted.customer_id
    AND trusted.flag_scd_active = True
    WHEN MATCHED
    AND (
        sub.customer_zip_code_prefix <> trusted.customer_zip_code_prefix 
        OR sub.customer_city <> trusted.customer_city
        OR sub.customer_state <> trusted.customer_state
    ) THEN UPDATE
    SET
        ts_end_date = current_timestamp(),
        flag_scd_active = False
    WHEN NOT MATCHED THEN INSERT
        (
            ref_day,
            ref_file_extraction,
            customer_id,
            customer_unique_id,
            customer_zip_code_prefix,
            customer_city,
            customer_state,
            ts_start_date,
            ts_end_date,
            flag_scd_active
        )
        VALUES
        (
            ref_day,
            ref_file_extraction,
            customer_id,
            customer_unique_id,
            customer_zip_code_prefix,
            customer_city,
            customer_state,
            current_timestamp(),
            null,
            True
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
            '0001_trusted_customers' as str_process, 
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

#temp code to test scd 2 in trusted

#raw_customers = spark.sql(
#"""
#    SELECT
#        ref_day,
#        ref_day_partition,
#        ref_file_extraction,
#        ref_file_extraction_partition,
#        customer_id,
#        customer_unique_id,
#        customer_zip_code_prefix,
#        case
#            when customer_id = "503840d4f2a1a7609f6489f44ffa9f7c" then "teste3"
#            else customer_city
#        end as customer_city,
#        customer_state
#    FROM raw_customers
#"""
#)
#
#raw_customers.write\
#    .partitionBy('ref_day_partition','ref_file_extraction_partition') \
#    .format("delta") \
#    .mode("overwrite") \
#    .save(str_s3_raw_file_path)
#
#raw_customers.createOrReplaceTempView("raw_customers")
