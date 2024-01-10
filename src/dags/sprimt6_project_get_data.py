from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import dag

import boto3
import pendulum


AWS_ACCESS_KEY_ID = "YCAJEWXOyY8Bmyk2eJL-hlt2K"
AWS_SECRET_ACCESS_KEY = "YCPs52ajb2jNXxOUsL4-pFDL1HnV2BCPd928_ZoA"

def group_log_s3_file(bucket: str, key: str) -> str:
    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )
    s3_client.download_file(
				Bucket=bucket, 
				Key=key, 
				Filename=f'/data/{key}'
)


bash_command_tmpl = """
head {{ files }}
"""

@dag(schedule_interval=None, start_date=pendulum.parse('2022-07-13'))
def sprint6_PROJECT_get_data():
    #bucket_files = ('group_log.csv')
    group_log_tasks = [
        PythonOperator(
            task_id=f'load_group_log',
            python_callable=group_log_s3_file,
            op_kwargs={'bucket': 'sprint6', 'key': 'group_log.csv'})
    ]
        
    group_log_tasks

_ = sprint6_PROJECT_get_data()


