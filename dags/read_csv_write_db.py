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
    'load_banking_data',
    default_args=default_args,
    description='DAG для загрузки банковских данных',
    schedule_interval=None,  
    catchup=False,
    tags=['etl', 'csv', 'bash', 'postgres']  
)



load_ft_balance_f = BashOperator(
    task_id='load_ft_balance_f',
    bash_command=f'python {scripts_path}/read_and_load_ft_balance_f.py',
    dag=dag,
)

load_ft_posting_f = BashOperator(
    task_id='load_ft_posting_f',
    bash_command=f'python {scripts_path}/read_and_load_ft_posting_f.py',
    dag=dag,
)

load_md_account_d = BashOperator(
    task_id='load_md_account_d',
    bash_command=f'python {scripts_path}/read_and_load_md_account_d.py',
    dag=dag,
)

load_md_currency_d = BashOperator(
    task_id='load_md_currency_d',
    bash_command=f'python {scripts_path}/read_and_load_md_currency_d.py',
    dag=dag,
)

load_md_exchange_rate_d = BashOperator(
    task_id='load_md_exchange_rate_d',
    bash_command=f'python {scripts_path}/read_and_load_md_exchange_rate_d.py',
    dag=dag,
)

load_md_ledger_account_s = BashOperator(
    task_id='load_md_ledger_account_s',
    bash_command=f'python {scripts_path}/read_and_load_md_ledger_account_s.py',
    dag=dag,
)


load_ft_balance_f >> load_ft_posting_f >> load_md_account_d >> load_md_currency_d >> load_md_exchange_rate_d >> load_md_ledger_account_s