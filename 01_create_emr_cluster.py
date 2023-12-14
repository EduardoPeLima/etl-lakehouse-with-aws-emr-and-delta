import boto3

def get_emr_client():
    client = boto3.client(
        'emr',
        region_name = 'us-east-1'
        )
    return client

def get_emr_list(emr_client):
    response = emr_client.list_clusters(ClusterStates=['STARTING', 'BOOTSTRAPPING', 'RUNNING', 'WAITING'])
    for cluster in response['Clusters']:
        if (cluster['Name'] == 'emr_ecommerce_project'):
            return cluster
    return None

def create_emr_cluster(emr_client, cluster_name, release_label, num_instances, instance_type, key_pair_name, log_uri):

    response = emr_client.run_job_flow(
        Name=cluster_name,
        ReleaseLabel=release_label,
        Instances={
            'MasterInstanceType': instance_type, 'SlaveInstanceType': instance_type,
            'InstanceCount': num_instances,
            'KeepJobFlowAliveWhenNoSteps': True,
            'Ec2KeyName': key_pair_name,
        },
        Applications=[
            {'Name': 'Spark'},
        ],
        VisibleToAllUsers=True,
        JobFlowRole='EMR_EC2_DefaultRole',
        ServiceRole='EMR_DefaultRole',
        LogUri=log_uri,
    )

    return response['JobFlowId']

emr_client = get_emr_client()
emr_ecommerce_cluster = get_emr_list(emr_client)

if (emr_ecommerce_cluster == None):
    print('Creating EMR Cluster')
    cluster_name = 'emr_ecommerce_project'
    release_label = 'emr-6.15.0'  
    num_instances = 1
    instance_type = 'm5.xlarge' 
    key_pair_name = 'key_pair_ecommerce_project' 
    log_uri = 's3://ecommerce-project-emr-logs'  
    cluster_id = create_emr_cluster(emr_client, cluster_name, release_label, num_instances, instance_type, key_pair_name, log_uri)
    print(f'EMR Cluster {cluster_id} is being created')
else:
    print('EMR Cluster emr_ecommerce_project already exists')