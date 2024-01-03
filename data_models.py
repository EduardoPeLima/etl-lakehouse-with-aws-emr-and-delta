import os
from datetime import datetime

class DataModel():
#boto3 by default consumes local variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in client
    
    def __init__(self, S3_AWS_CLIENT, INGESTION_BUCKET):
        self.S3_AWS_CLIENT = S3_AWS_CLIENT
        self.INGESTION_BUCKET = INGESTION_BUCKET

    def __ensure_s3_bucket_exists(self):
        try:
            try:
                self.S3_AWS_CLIENT.head_bucket(Bucket=self.INGESTION_BUCKET)
            except:
                self.S3_AWS_CLIENT.create_bucket(
                    Bucket=self.INGESTION_BUCKET,
                )
                print(f'Bucket {self.INGESTION_BUCKET} created')
            else:
                print(f'bucket {self.INGESTION_BUCKET} already exists')
            
        except Exception as e:
            print(f'Failed to create bucket: {e}')

    def ingest_local_data_to_s3(self):

        self.__ensure_s3_bucket_exists()

        print('Target Bucket for Data Ingestion: ', self.INGESTION_BUCKET)

        folder_path = "original_data"
        files = os.listdir(folder_path)
        current_datetime = datetime.today().strftime("%Y%m%d%H%M%S")

        for file in files:
            file_path = os.path.join(folder_path, file)

            file_name = (file.replace('.csv','')).lower()
            self.S3_AWS_CLIENT.upload_file(file_path, self.INGESTION_BUCKET, f'ecommerce/{file_name}_{current_datetime}.csv')
            print(f'{file} uploaded to S3')
        
        print('All files were uploaded to S3')