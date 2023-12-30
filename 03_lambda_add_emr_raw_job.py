import boto3 
import os
import pandas as pd
from io import StringIO
import re

def get_emr_client():
    client = boto3.client(
        'emr',
        region_name = 'us-east-1'
        )
    return client

def get_s3_client():
    client = boto3.client(
        's3',
        region_name = 'us-east-1'
        )
    return client

def get_emr_list(emr_client):
    response = emr_client.list_clusters(ClusterStates=['STARTING', 'BOOTSTRAPPING', 'RUNNING', 'WAITING'])
    for cluster in response['Clusters']:
        #pegar o nome do cluster de acordo com o projeto, adicionar coluna 'projeto' na control
        if (cluster['Name'] == 'emr_ecommerce_project'):
            return cluster
    return None

def add_spark_job_to_emr(cluster_id, s3_script_path):
    emr_client = boto3.client('emr', region_name='us-east-1')

    step = [
        {
            'Name': f'submiting spark job: {s3_script_path}',
            'ActionOnFailure': 'CONTINUE', 
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': ['spark-submit', "--deploy-mode", "client", s3_script_path],
            }
        }
    ]

    response = emr_client.add_job_flow_steps(
        JobFlowId=cluster_id,
        Steps=step
    )

    print('Step job added')
    return response

def find_spark_job(s3_client, file_name):
    BUCKET_CONTROL_NAME = os.getenv('AWS_BUCKET_CONTROL_NAME')
    TRANSFER_CONTROL_S3_KEY = 'transfer_control.csv'

    response = s3_client.get_object(Bucket=BUCKET_CONTROL_NAME, Key=TRANSFER_CONTROL_S3_KEY)
    csv_content = response['Body'].read().decode('utf-8')

    df = pd.read_csv(StringIO(csv_content))

    search_regex = re.escape(file_name)
    print(search_regex)

    for index, row in df.iterrows():
        if re.search(row['regex_file_name'], search_regex, re.IGNORECASE):
           return row['spark_job_s3_key']
    else:
        raise Exception(f'Spark job was not found inside transfer control. Key {TRANSFER_CONTROL_S3_KEY}')
    
file_name = "olist_customers_dataset_20231214170632.csv"

emr_client = get_emr_client()
s3_client = get_s3_client()
emr_ecommerce_cluster = get_emr_list(emr_client)
spark_job_s3_path = find_spark_job(s3_client, file_name)

add_spark_job_to_emr(emr_ecommerce_cluster['Id'], spark_job_s3_path)