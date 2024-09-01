from datetime import timedelta 
from airflow import DAG 
from airflow.operators.python import PythonOperator  
from airflow.utils.dates import days_ago
from datetime import datetime
import glob
from twitter_etl import run_twitter_etl


default_args = {
   'owner' : 'airflow' ,
   'depends_on_past': False,
   'start_date': datetime(2024, 8, 23),
   'email' : ['adeoluwaadegboye1@gmail.com'],
   'email_on_failure': False, 
   'email_on_retry': False,
   'retries': 1,
   'retry_delay' : timedelta(minutes=1)
}


dag = DAG(
    'twitter_dag', 
    default_args = default_args, 
    description = 'My first etl code',
    schedule_interval=timedelta(days=1),  # Set schedule interval as needed
    )

run_etl = PythonOperator(
    task_ids = 'complete_twitter_etl',
    python_callable = run_twitter_etl
    dag = dag
)

run_etl