# Brazilian Ecommerce Data Engineering Project
### Intro
<p>
This project uses a data available from Klagle, about a Brazilian ecommerce public dataset of orders made at Olist Store. The dataset has information of 100k orders from 2016 to 2018 made at multiple marketplaces in Brazil. Its features allows viewing an order from multiple dimensions, like location, customers and products. This is real commercial data and because of it has been anonymised.
</p>
<p>
The chosen data stack is well-known and established in the industry, where our objective is to build a robust Data Lakehouse, leveraging services from AWS and Delta Lake, developed by Databricks and now open source.
</p>

### Data Architecture
![Project Architecture](imgs/aws_data_architecture.png)

### Data Ingestion and Transformation
<p>
A Python script called "orchestrator.py" is responsible to orchestrate the ingestion and spark transformation jobs in our data pipeline. The utilization of the lib boto3 in Python is very important to manipulate the AWS Resources automacally.
</p>
<ul>
    <li>All the csv files in our on-premisse machine is send to our landzone bucket in S3.</li>
    <li>Then, our AWS EMR Cluster with Spark, JupyterHub, Livy and Hadoop is created. The Delta Lake is not a simple option to use at EMR when creating it, so it is necessary for our orchestrator to execute a boostrap action to copy the Delta Core and Storage JARS from our S3 and install him correctly in our cluster.</li>
    <li>The cluster creation take time from 5 until 9 minutes, and because of that, it is necessary to our orchestrator create and monitoring until the cluster is totally created. When the orchestrator checks that the cluster is created and waiting to jobs, it submits our spark jobs saved in S3 to the cluster and the data transformations really begins.
    </li>
    <li> The raw sparks jobs are responsable to collect data from landzone and append in raw zone in Delta Lake format. After the raw zone is completely processed, the trusted spark jobs reads the new data from raw zone and merge in trusted zone updating and inserting new data. </li>
    <i>The data in the landzone is deleted after completely processed to raw zone without errors.</i>
    <i>In raw zone and so on, the data are already saved in Delta format, enabling ACID transactions, schema enforcement, time travel etc.</i>
</ul>

![Spark Jobs](imgs/emr_spark_jobs_raw_trusted.png.png)

<ul>
<li>After the trusted zone is completely processed, the refined sparks jobs starts, creating the star schema tables (dimensions with SCD 1, SCD 2 and facts) and aggregated tables from the trusted tables.</li>
</ul>

![Spark Jobs](imgs/emr_spark_jobs_refined.png)

### Serving to our end-users the trusted and refined data
<p>

</p>