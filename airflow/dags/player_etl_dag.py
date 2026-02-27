from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from src.extract.download_dataset import main

#Defining DAG arguments
default_args = {
    'owner': 'FMaravilha',
    'start_date': datetime(2026,2,27),
    'email': 'franciscojmasantos@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    }

with DAG(
    'Player_Stats',
    default_args=default_args,
    description='Player stats and market value.',
    schedule="@weekly", #the dataset is updated weekly
    catchup= False
) as dag:

    extract_dataset_from_kaggle=PythonOperator(
        task_id='extract_dataset',
        python_callable= main,
        dag=dag
    )

