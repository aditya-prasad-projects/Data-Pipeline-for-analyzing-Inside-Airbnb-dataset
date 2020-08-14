from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.S3_hook import S3Hook
import logging
import boto3
import pandas as pd
import requests
import io

class LoadToS3Operator(BaseOperator):
    
    @apply_defaults
    def __init__(self,
                 aws_credentials_id="",
                 s3_bucket="",
                 url_key="",
                 output_key="",
                 *args, **kwargs):

        super(LoadToS3Operator, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.url_key = url_key
        self.output_key = output_key
        
    def execute(self, context):
        self.log.info('LoadToS3 Operator is now in progress')

        # get the hooks
        hook = S3Hook(aws_conn_id=self.aws_credentials_id)
        bucket = hook.get_bucket(self.s3_bucket)
        url = hook.get_key(self.url_key, self.s3_bucket)
        url_df = pd.read_csv(io.BytesIO(url.get()['Body'].read()))
        for index, row in url_df.iterrows():
            split_url = row["url"].split("/")
            bucket_key = "raw_data/" + ("/").join(split_url[3:7] + [split_url[-1]])
            self.log.info(bucket_key)
            response = requests.get(row["url"], stream=True)
            hook.load_file_obj(response.raw, bucket_key, self.s3_bucket)

        
        
        
        