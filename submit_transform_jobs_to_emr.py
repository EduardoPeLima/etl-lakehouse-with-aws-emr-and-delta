import boto3 
import os
import pandas as pd
from io import StringIO
import re

class SubmitJobsToEMR():

    def __init__(self, EMR_CLIENT, cluster_id):
        self.EMR_CLIENT = EMR_CLIENT
        self.cluster_id = cluster_id

    def add_spark_job_to_emr(self, s3_script_path):

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

        response = self.EMR_CLIENT.add_job_flow_steps(
            JobFlowId=self.cluster_id,
            Steps=step
        )

        print(f'Step {s3_script_path} job added')
        return response