import os
import sys
from datetime import datetime,timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))



from pipelines.socialstream_pipeline import socialstream_pipeline

# Default arguments
default_args = {
    'owner': 'Atharv-Nanaware',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email': ['atharvnanaware01@gmail.com'],
}

file_postfix = datetime.now().strftime("%Y%m%d")


dag = DAG(
    dag_id='etl_socialstream_pipeline',  #
    default_args=default_args,
    description='An ETL pipeline for extracting, processing, and uploading  data to AWS S3',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    tags=['social stream', 'etl', 'pipeline']  #
)

# Task 1: Extract data from Reddit
extract_reddit_task = PythonOperator(
    task_id='extract_reddit_data',
    python_callable=socialstream_pipeline,
    op_kwargs={
        'file_name': f'socialstream_{file_postfix}',
        'subreddit': 'science+politics+technology+relationships',
        'time_filter': 'all',
        'limit': 5000
    },
    dag=dag,
)


