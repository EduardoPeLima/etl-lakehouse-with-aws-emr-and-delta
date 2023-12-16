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
        #pegar o nome do cluster de acordo com o projeto, adicionar coluna 'projeto' na control
        if (cluster['Name'] == 'emr_ecommerce_project'):
            return cluster
    return None

def add_emr_step(cluster_id, s3_script_path):
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
    

emr_client = get_emr_client()
emr_ecommerce_cluster = get_emr_list(emr_client)

add_emr_step(emr_ecommerce_cluster['Id'], 's3://ecommerce-project-control/jupyter/jovyan/raw/0001_raw_customers.py')