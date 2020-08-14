import datetime

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (UnzipS3Operator, LoadToS3Operator)
from airflow.operators.dummy_operator import DummyOperator

def get_data_transfer_dag(parent_dag_name,
        task_id,
        aws_credentials_id="",
        s3_bucket="",
        output_key="",
        url_key="",
        *args, **kwargs):
    
    dag = DAG(
        f"{parent_dag_name}.{task_id}",
        *args, **kwargs
    )
    
    begin_execution_task = DummyOperator(task_id='Begin_execution', dag=dag)

    load_task = LoadToS3Operator(
            task_id="load_s3",
            aws_credentials_id=aws_credentials_id,
            s3_bucket=s3_bucket,
            output_key='raw_data',
            url_key=url_key,
            dag=dag
    )

    unzip_task = UnzipS3Operator(
            task_id="unzip_s3",
            aws_credentials_id=aws_credentials_id,
            s3_bucket=s3_bucket,
            output_key=output_key,
            dag=dag
    )

    end_execution_task = DummyOperator(task_id='End_execution', dag=dag)

    begin_execution_task >> load_task

    load_task >> unzip_task

    unzip_task >> end_execution_task
    
    return dag
    
    
        
        
        
    
    
