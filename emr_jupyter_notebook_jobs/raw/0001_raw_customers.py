import re
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from datetime import datetime, timedelta

spark = SparkSession.builder.appName('0001_raw_customers').getOrCreate()
sqlContext=SQLContext(spark.sparkContext)

#prefix list

#dt: datetime
#str: string

#cache and count are commented because they were used just to make the tests faster, saving cache and activating a Spark action.

str_bucket_landzone = "ecommerce-project-landzone"
str_bucket_raw = "ecommerce-project-raw"
str_bucket_control = "ecommerce-project-control"

dt_proc_brazilian = datetime.now() - timedelta(hours=3)
str_proc_brazilian_datetime = dt_proc_brazilian.strftime("%Y%m%d%H%M%S")

str_landzone_file_path = "ecommerce/olist_customers_dataset_20231214170632.csv"
str_file_name = str_landzone_file_path.split("/")[-1]
print(str_file_name)

regex_file_extract_pattern = re.compile(r'(\d{14})')
str_file_extract_datetime = regex_file_extract_pattern.search(str_file_name).group(1)
print(str_file_extract_datetime)

str_raw_file_path = "ecommerce/olist_customers_dataset"

str_s3_landzone_file_path = f's3://{str_bucket_landzone}/{str_landzone_file_path}'
print(str_s3_landzone_file_path)

landzone_customers = spark.read.format(
    "com.databricks.spark.csv").option(
    "header","true").option(
    "encoding","UTF-8").option(
    "inferSchema","false").option(
    "delimiter",",").load(
    str_s3_landzone_file_path)

landzone_customers.createOrReplaceTempView("landzone_customers")

#landzone_customers.cache()
#qtd=landzone_customers.count()
#print('rows from landzone file: ', qtd)

raw_customers = spark.sql(
    f"""
        SELECT
        
            --default datalake metadata
            int(date_format(current_date, 'yyyymmdd')) as ref_day,
            int(date_format(current_date, 'yyyymmdd')) as ref_day_partition,
            "{str_file_extract_datetime}" as dt_file_extraction,
            "{str_file_extract_datetime}" as dt_file_extraction_partition,
            
            --file fields
            customer_id,
            customer_unique_id,
            customer_zip_code_prefix,
            customer_city,
            customer_state
        FROM landzone_customers
    """
)

raw_customers.createOrReplaceTempView('raw_customers')
#raw_customers.cache()
#raw_customers.count()

str_raw_path_file = f's3://{str_bucket_raw}/{str_raw_file_path}'

raw_customers.coalesce(3).write.partitionBy('ref_day_partition','dt_file_extraction').parquet(str_raw_path_file,mode="append")
print('file uploaded at: ', str_raw_path_file)

control = spark.sql(
    f"""
        SELECT
            "{str_bucket_landzone}" as str_origin_zone,
            "{str_bucket_raw}" as str_target_zone,
            '0001_raw_customers' as str_process, 
            "{str_landzone_file_path}" as str_origin_file_path,
            "{str_raw_file_path}" as str_target_file_path,
            ref_day as ref,
            ref_day_partition as ref_partition,
            dt_file_extraction,
            dt_file_extraction_partition,
            "{str_proc_brazilian_datetime}" as dt_proc,
            count(*) as nu_qtd_rows
        FROM raw_customers
        GROUP BY 1,2,3,4,5,6,7,8,9,10
    """
)

#control.createOrReplaceTempView('control')
#control.cache()
#control.count()

str_control_path = f's3://{str_bucket_control}/tb_0001_control_process_raw'
print(str_control_path)

control.coalesce(1).write.parquet(str_control_path,mode="append")
