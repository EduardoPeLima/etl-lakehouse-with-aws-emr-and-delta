import boto3
import time

class Clients():
    #there are no need to add AWS SECRET KEY and AWS ACCESS KEY here, because they are on environment variables and boto3 consumes by default
    def __init__(self):
        pass

    def get_s3_client(self):
        s3_client = boto3.client(
            "s3"
        )
        print("S3 AWS Client Created")
        return s3_client
    
    def get_emr_client(self):
        client = boto3.client(
            'emr',
            region_name = 'us-east-1'
            )
        print("EMR AWS Client Created")
        return client

class EmrEcommerce():

    def __init__(self, emr_client : str, cluster_name : str, release_label : str, num_instances : int, instance_type : str, key_pair_name : str, log_uri : str):
        self.emr_client = emr_client
        self.cluster_name = cluster_name
        self.release_label = release_label
        self.num_instances = num_instances
        self.instance_type = instance_type
        self.key_pair_name = key_pair_name
        self.log_uri = log_uri
    
    def get_ecommerce_cluster_id(self):

        emr_ecommerce_check_qtd = 1
        emr_ecommerce_state_created = False

        if self.__check_emr_ecommerce_cluster() == None:
            self.__create_emr_cluster()
        
        while emr_ecommerce_state_created == False:
            print(f'Waiting EMR Ecommerce state to WAITING. Attempt: {emr_ecommerce_check_qtd}...')     

            response = self.emr_client.list_clusters(ClusterStates=['RUNNING', 'WAITING'])

            for cluster in response['Clusters']:
                if (cluster['Name'] == 'emr_ecommerce_project'):
                    print(f'EMR Ecommerce created, ID: {cluster['Id']}')
                    emr_ecommerce_state_created = True   

            if emr_ecommerce_state_created == False:
                time.sleep(60)
                emr_ecommerce_check_qtd = emr_ecommerce_check_qtd + 1   

        return cluster['Id']
    
    def __check_emr_ecommerce_cluster(self):
        response = self.emr_client.list_clusters(ClusterStates=['STARTING', 'BOOTSTRAPPING', 'RUNNING', 'WAITING'])
        for cluster in response['Clusters']:
            if (cluster['Name'] == 'emr_ecommerce_project'):
                return cluster['Id']
        return None

    def __create_emr_cluster(self):

        response = self.emr_client.run_job_flow(
            Name=self.cluster_name,
            ReleaseLabel=self.release_label,
            Instances={
                'MasterInstanceType': self.instance_type, 
                'SlaveInstanceType': self.instance_type,
                'InstanceCount': self.num_instances,
                'KeepJobFlowAliveWhenNoSteps': True,
                'Ec2KeyName': self.key_pair_name,
            },
            Configurations= [
                    {
                        "Classification": "jupyter-s3-conf",
                        "Properties": {
                            "s3.persistence.enabled": "true",
                            "s3.persistence.bucket": "ecommerce-project-control"
                        }
                    }
            ],
            BootstrapActions=[
            {
                'Name': 'string',
                'ScriptBootstrapAction': {
                    'Path': 's3://spark-addons/emr_bootstrap.sh',
                    'Args': []
                }
            },
            ],
            #we don't need to specify the applications versions, because we are already using a EMR release label
            Applications=[ 
                {
                    'Name': 'Spark',
                },
                {
                    'Name': 'Livy',
                },
                {
                    'Name': 'Hadoop',
                },
                {
                    'Name': 'JupyterHub',
                }
            ],
            VisibleToAllUsers=True,
            JobFlowRole='EMR_EC2_DefaultRole',
            ServiceRole='EMR_DefaultRole',
            LogUri=self.log_uri,
        )

        return response['JobFlowId']