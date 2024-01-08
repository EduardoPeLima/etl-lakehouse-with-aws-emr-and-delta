import os

from data_models import DataModel
from aws import Clients, EmrEcommerce
from submit_transform_jobs_to_emr import SubmitJobsToEMR

def main():
    INGESTION_BUCKET = os.getenv('AWS_BUCKET_LANDZONE_NAME')

    get_aws_clients = Clients()
    S3_CLIENT = get_aws_clients.get_s3_client()
    EMR_CLIENT = get_aws_clients.get_emr_client()

    data_model = DataModel(S3_AWS_CLIENT=S3_CLIENT, INGESTION_BUCKET=INGESTION_BUCKET)
    #data_model.ingest_local_data_to_s3()

    cluster_name = "emr_ecommerce_project"
    release_label = "emr-6.15.0"
    num_instances = 1
    instance_type = "m5.xlarge"
    key_pair_name = "key_pair_ecommerce_project" 
    log_uri = "s3://ecommerce-project-emr-logs" 
    aws_emr_ecommerce = EmrEcommerce(EMR_CLIENT, cluster_name, release_label, num_instances, instance_type, key_pair_name, log_uri)
    aws_emr_ecommerce_id = aws_emr_ecommerce.get_ecommerce_cluster_id()

    submit_transform_jobs_to_emr = SubmitJobsToEMR(EMR_CLIENT, aws_emr_ecommerce_id)
    #raw
    #submit_transform_jobs_to_emr.add_spark_job_to_emr('s3://ecommerce-project-control/jupyter/jovyan/raw/0001_raw_customers.py')

    #trusted
    submit_transform_jobs_to_emr.add_spark_job_to_emr('s3://ecommerce-project-control/jupyter/jovyan/trusted/0001_trusted_customers.py')


    #gold
    


if __name__ == "__main__":
    main()