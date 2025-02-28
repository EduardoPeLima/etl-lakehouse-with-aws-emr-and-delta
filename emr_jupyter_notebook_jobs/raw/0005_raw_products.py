import re
import os
from datetime import datetime, timedelta
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import input_file_name, col

spark = SparkSession.builder \
    .appName('0005_raw_products') \
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

str_bucket_landzone = "ecommerce-project-landzone"
str_bucket_raw = "ecommerce-project-raw"
str_bucket_control = "ecommerce-project-control"

ts_proc = datetime.now()
str_proc_timestamp = ts_proc.strftime("%Y%m%d%H%M%S")

str_landzone_file_path = "ecommerce/olist_products_dataset/"

str_raw_file_path = "ecommerce/olist_products_dataset"

str_s3_landzone_file_path = f's3://{str_bucket_landzone}/{str_landzone_file_path}'
print(str_s3_landzone_file_path)

landzone_products = spark.read.format(
    "com.databricks.spark.csv").option(
    "header","true").option(
    "encoding","UTF-8").option(
    "inferSchema","false").option(
    "delimiter",",").load(
    str_s3_landzone_file_path)

landzone_products.createOrReplaceTempView("landzone_products")

raw_products = spark.sql(
    f"""
        SELECT
        
            --default datalake metadata
            int(date_format(current_timestamp(), 'yyyyMMdd')) as ref_day,
            int(date_format(current_timestamp(), 'yyyyMMdd')) as ref_day_partition,
            cast(
                replace(reverse(substring_index(reverse(input_file_name()), '_', 1)),'.csv','') 
            as long) as ref_file_extraction,
            cast(
                replace(reverse(substring_index(reverse(input_file_name()), '_', 1)),'.csv','')
            as long) as ref_file_extraction_partition,
            
            --file fields
            string(product_id),
            string(product_category_name),
            string(product_name_lenght),
            string(product_description_lenght),
            string(product_photos_qty),
            string(product_weight_g),
            string(product_length_cm),
            string(product_height_cm),
            string(product_width_cm)
        FROM landzone_products
    """
)

raw_products.createOrReplaceTempView('raw_products')

str_raw_path_file = f's3://{str_bucket_raw}/{str_raw_file_path}'

raw_products.write \
    .partitionBy('ref_day_partition','ref_file_extraction_partition') \
    .format("delta") \
    .mode("append") \
    .save(str_raw_path_file)

print('file uploaded at: ', str_raw_path_file)

control = spark.sql(
    f"""
        SELECT
            "{str_bucket_landzone}" as str_origin_zone,
            "{str_bucket_raw}" as str_target_zone,
            '0005_raw_products' as str_process, 
            "{str_landzone_file_path}" as str_origin_file_path,
            "{str_raw_file_path}" as str_target_file_path,
            ref_day as ref,
            ref_day_partition as ref_partition,
            ref_file_extraction,
            ref_file_extraction_partition,
            int("{str_proc_timestamp}") as dt_proc,
            count(*) as nu_qtd_rows
        FROM raw_products
        GROUP BY 1,2,3,4,5,6,7,8,9,10
    """
)

#control.createOrReplaceTempView('control')
#control.cache()
#control.count()

str_control_path = f's3://{str_bucket_control}/tb_0001_control_process_raw'
print(str_control_path)

control.write \
    .format('delta') \
    .mode('append') \
    .save(str_control_path)

print('Log appended to control')

cmd=f'aws s3 rm {str_s3_landzone_file_path} --recursive > /dev/null'
os.system(cmd)
print('Processed landzone cleaned')


