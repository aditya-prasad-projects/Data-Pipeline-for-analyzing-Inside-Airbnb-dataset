from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.S3_hook import S3Hook
import logging
import boto3

import gzip
import io

class UnzipS3Operator(BaseOperator):
    
    @apply_defaults
    def __init__(self,
                 aws_credentials_id="",
                 s3_bucket="",
                 output_key="",
                 *args, **kwargs):

        super(UnzipS3Operator, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.output_key = output_key
    
    def get_uncompressed_key(self, compressed_key):
        index_folder = compressed_key.index("/")
        if(compressed_key.endswith(".gz")):
            return self.output_key + compressed_key[index_folder:-3]
        return self.output_key + compressed_key[index_folder:]
        
    def execute(self, context):
        self.log.info('LoadToS3 Operator is now in progress')

        # get the hooks
        hook = S3Hook(aws_conn_id=self.aws_credentials_id)
        bucket = hook.get_bucket(self.s3_bucket)
        for obj in bucket.objects.all():
            uncompressed_key = self.get_uncompressed_key(obj.key)
            self.log.info(uncompressed_key)
            if obj.key.endswith('.gz'):
                bucket.upload_fileobj(                      # upload a new obj to s3
                        Fileobj=gzip.GzipFile(              # read in the output of gzip -d
                            None,                           # just return output as BytesIO
                            'rb',                           # read binary
                            fileobj=io.BytesIO(obj.get()['Body'].read())
                        ),
                        Key=uncompressed_key) 
            else:
                hook.load_file_obj(obj.get()['Body'], uncompressed_key, self.s3_bucket)

    

        
        
        
        