import re
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import lit, col
from datetime import datetime, timedelta

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

from delta.tables import *

#sqlContext=SQLContext(spark.sparkContext)

%%configure -f
{
  "conf": {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    "spark.jars.packages": "io.delta:delta-storage-2.4.0"
  }
}

#prefix list

#dt: datetime
#str: string

#cache and count are commented because they were used just to make the tests faster, saving cache and activating a Spark action.

str_bucket_raw = "ecommerce-project-raw"
str_bucket_trusted = "ecommerce-project-trusted"
str_bucket_control = "ecommerce-project-control"

dt_proc_brazilian = datetime.now() - timedelta(hours=3)
str_proc_brazilian_datetime = dt_proc_brazilian.strftime("%Y%m%d%H%M%S")

key_file_path = "ecommerce/olist_customers_dataset"

str_s3_raw_file_path = f's3://{str_bucket_raw}/{key_file_path}'
print(str_s3_raw_file_path)

raw_customers = spark.read.format("delta").load(str_s3_raw_file_path)
raw_customers = raw_customers.withColumn('join_key',lit(""))

raw_customers.createOrReplaceTempView("raw_customers")

raw_customers.cache()
qtd=raw_customers.count()
print('rows from landzone file: ', qtd)

#SCD2 will be utilized, and must happen a new register for a customer when his 
#customer_zip_code_prefix, customer_city or customer_state has changed.

##célula temporária para simular que a raw_customers tem uma alteração de cidade

raw_customers = spark.sql(
"""
    SELECT
        ref_day,
        ref_day_partition,
        ref_file_extraction,
        ref_file_extraction_partition,
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix,
        case
            when customer_id = "503840d4f2a1a7609f6489f44ffa9f7c" then "teste2"
            else customer_city
        end as customer_city,
        customer_state
    FROM raw_customers
"""
)

raw_customers.write\
    .partitionBy('ref_day_partition','ref_file_extraction_partition') \
    .format("delta") \
    .mode("overwrite") \
    .save(str_s3_raw_file_path)

str_s3_trusted_file_path = f's3://{str_bucket_trusted}/{key_file_path}'
print(str_s3_trusted_file_path)

spark.sql(
"""
select 
    trusted.*,
    raw.customer_id as rola,
    raw.customer_city as teste
from delta.`s3://ecommerce-project-trusted/ecommerce/olist_customers_dataset` as trusted
left join delta.`s3://ecommerce-project-raw/ecommerce/olist_customers_dataset` as raw
on raw.customer_id = trusted.customer_id
where trusted.customer_id = "503840d4f2a1a7609f6489f44ffa9f7c"
"""
).show(5)

+--------+-------------------+--------------------+--------------------+------------------------+-------------+--------------+--------------------+--------------------+---------------+--------------------+------+
| ref_day|ref_file_extraction|         customer_id|  customer_unique_id|customer_zip_code_prefix|customer_city|customer_state|       ts_start_date|         ts_end_date|flag_scd_active|                rola| teste|
+--------+-------------------+--------------------+--------------------+------------------------+-------------+--------------+--------------------+--------------------+---------------+--------------------+------+
|20240104|               null|503840d4f2a1a7609...|ffc4233210eac4ec1...|                   14811|   araraquara|            SP|2024-01-05 02:15:...|2024-01-06 17:33:...|          false|503840d4f2a1a7609...|teste2|
|20240104|               null|503840d4f2a1a7609...|ffc4233210eac4ec1...|                   14811|        teste|            SP|2024-01-06 17:06:...|2024-01-06 17:33:...|          false|503840d4f2a1a7609...|teste2|
|20240104|               null|503840d4f2a1a7609...|ffc4233210eac4ec1...|                   14811|       teste2|            SP|2024-01-06 17:33:...|                null|           true|503840d4f2a1a7609...|teste2|
+--------+-------------------+--------------------+--------------------+------------------------+-------------+--------------+--------------------+--------------------+---------------+--------------------+------+

trusted_customers_now = DeltaTable.forPath(
        spark, str_s3_trusted_file_path)

spark.sql(
"""
    MERGE INTO delta.`s3://ecommerce-project-trusted/ecommerce/olist_customers_dataset` as trusted
    USING
    (
        SELECT
            raw.customer_id as join_key,
            raw.ref_day,
            raw.ref_file_extraction,
            raw.customer_id,
            raw.customer_unique_id,
            raw.customer_zip_code_prefix,
            raw.customer_city,
            raw.customer_state
        FROM raw_customers as raw
        UNION ALL
        SELECT
            NULL as join_key,
            raw.ref_day,
            raw.ref_file_extraction,
            raw.customer_id,
            raw.customer_unique_id,
            raw.customer_zip_code_prefix,
            raw.customer_city,
            raw.customer_state
        FROM raw_customers as raw
        INNER JOIN delta.`s3://ecommerce-project-trusted/ecommerce/olist_customers_dataset` as trusted
        ON raw.customer_id = trusted.customer_id
        WHERE
            (
            raw.customer_zip_code_prefix <> trusted.customer_zip_code_prefix 
            OR raw.customer_city <> trusted.customer_city
            OR raw.customer_state <> trusted.customer_state
            ) 
            AND trusted.flag_scd_active = True      
    ) as sub
    ON sub.join_key = trusted.customer_id
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





############ZONA PARA TESTAR O SCD 2

#celula temporaria para criar primeira trusted customers no S3

trusted_customers = spark.sql(
"""
    SELECT
        ref_day,
        ref_file_extraction,
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state,
        current_timestamp() as ts_start_date,
        CAST(NULL AS TIMESTAMP) as ts_end_date,
        True as flag_scd_active
    FROM raw_customers
"""
)

trusted_customers.show(5)

trusted_customers.write \
    .mode('overwrite') \
    .format('delta').save(str_s3_trusted_file_path)

df_teste_scd2 = spark.read.format('delta').load(str_s3_trusted_file_path)

df_teste_scd2.show(5, truncate=False)

+--------+-------------------+--------------------------------+--------------------------------+------------------------+---------------------+--------------+-----------------------+-----------+---------------+
|ref_day |ref_file_extraction|customer_id                     |customer_unique_id              |customer_zip_code_prefix|customer_city        |customer_state|ts_start_date          |ts_end_date|flag_scd_active|
+--------+-------------------+--------------------------------+--------------------------------+------------------------+---------------------+--------------+-----------------------+-----------+---------------+
|20240104|null               |06b8999e2fba1a1fbc88172c00ba8bc7|861eff4711a542e4b93843c6dd7febb0|14409                   |franca               |SP            |2024-01-05 02:15:44.496|null       |true           |
|20240104|null               |18955e83d337fd6b2def6b18a428ac77|290c77bc529b7ac935b93aa66c333dc3|09790                   |sao bernardo do campo|SP            |2024-01-05 02:15:44.496|null       |true           |
|20240104|null               |4e7b3e00288586ebd08712fdd0374a03|060e732b5b29e8181a18229c7b0b2b5e|01151                   |sao paulo            |SP            |2024-01-05 02:15:44.496|null       |true           |
|20240104|null               |b2b6027bc5c5109e529d4dc6358b12c3|259dac757896d24d7702b9acbbff3f3c|08775                   |mogi das cruzes      |SP            |2024-01-05 02:15:44.496|null       |true           |
|20240104|null               |4f2d8ab171c80ec8364f7c12e35b23ad|345ecd01c38d18a9036ed96c73b8d066|13056                   |campinas             |SP            |2024-01-05 02:15:44.496|null       |true           |
+--------+-------------------+--------------------------------+--------------------------------+------------------------+---------------------+--------------+-----------------------+-----------+---------------+

###teste no customer_id "503840d4f2a1a7609f6489f44ffa9f7c"


