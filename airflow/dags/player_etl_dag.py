from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from src.extract.download_dataset import main as main_extract
from src.validate.validate_raw import main as main_validate_raw
from src.transform.transform_load_staging import main as main_transform

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
    dag_id='transfermarkt_player_pipeline',
    default_args=default_args,
    description='Player stats and market value.',
    schedule="@weekly", #the dataset is updated weekly
    catchup= False
) as dag:

    extract_dataset_from_kaggle=PythonOperator(
        task_id='extract_dataset',
        python_callable= main_extract,
    )

    validate_raw_data=PythonOperator(
        task_id='validate_raw_data',
        python_callable= main_validate_raw,
    )

    transform_and_load_staging=PythonOperator(
        task_id='transform_raw_data',
        python_callable= main_transform,
    )

extract_dataset_from_kaggle >> validate_raw_data >> transform_and_load_staging


