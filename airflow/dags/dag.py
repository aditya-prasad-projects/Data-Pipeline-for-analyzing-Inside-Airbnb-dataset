from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.dummy_operator import DummyOperator
from data_transfer_S3_subdag import get_data_transfer_dag

start_date = datetime.utcnow()

default_args = {
    'owner': 'aditya',
    'depends_on_past': False, 
    'catchup_by_default': False,
    'provide_context': True,
}

dag_name = "test"

with DAG(
    dag_id = dag_name,
    start_date=start_date,
    default_args=default_args
) as dag:
    
    begin_execution_task = DummyOperator(task_id='Begin_execution')

    data_transfer_task_id = "data_transfer_S3"
    data_transfer_task = SubDagOperator(
        subdag=get_data_transfer_dag(
            parent_dag_name="test",
            task_id="data_transfer_S3",
            aws_credentials_id="aws_credentials",
            s3_bucket="test-url",
            output_key="unzipped",
            url_key="url/urls.csv",
            start_date=start_date
        ),
        task_id=data_transfer_task_id
    )

    end_execution_task = DummyOperator(task_id='End_execution')

    begin_execution_task >> data_transfer_task
    data_transfer_task >> end_execution_task

