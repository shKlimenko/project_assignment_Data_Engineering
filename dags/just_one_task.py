from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
import os

scripts_path = '/opt/airflow/scripts'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 24),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'just_one_task',
    default_args=default_args,
    description='DAG для проверки замены',
    schedule_interval=None,  
    catchup=False,
    tags=['csv', 'change', 'postgres']  
)



load_ft_balance_f = BashOperator(
    task_id='load_ft_balance_f',
    bash_command=f'python {scripts_path}/read_and_load_ft_balance_f.py',
    dag=dag,
)


load_ft_balance_f