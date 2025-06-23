from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

def load_csv_to_postgres():
    # Путь к папке с данными
    data_folder = '/path/to/your/data/folder'  # Замените на реальный путь
    csv_file = 'your_file.csv'  # Замените на имя вашего файла
    
    # Полный путь к файлу
    file_path = os.path.join(data_folder, csv_file)
    
    # Чтение CSV файла
    df = pd.read_csv(file_path)
    
    # Подключение к PostgreSQL
    postgres_hook = PostgresHook(postgres_conn_id='your_postgres_conn_id')
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()
    
    # Создание таблицы (если не существует)
    # Замените на вашу схему и имя таблицы
    table_name = 'your_table_name'
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        -- Определите здесь столбцы в соответствии с вашим CSV
        -- Пример:
        id SERIAL PRIMARY KEY,
        column1 VARCHAR(255),
        column2 INTEGER,
        -- ...
    )
    """
    cursor.execute(create_table_query)
    conn.commit()
    
    # Вставка данных
    # Преобразуем DataFrame в список кортежей
    data_tuples = [tuple(x) for x in df.to_numpy()]
    
    # Генерируем строку с плейсхолдерами (%s, %s, ...)
    cols = ','.join(list(df.columns))
    placeholders = ','.join(['%s'] * len(df.columns))
    
    # SQL запрос для вставки
    insert_query = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})"
    
    # Выполняем вставку
    cursor.executemany(insert_query, data_tuples)
    conn.commit()
    
    # Закрываем соединение
    cursor.close()
    conn.close()

with DAG(
    'load_csv_to_postgres',
    default_args=default_args,
    schedule_interval=None,  # или укажите расписание, например '0 0 * * *' для ежедневного выполнения
    catchup=False,
) as dag:
    
    load_data_task = PythonOperator(
        task_id='load_csv_to_postgres',
        python_callable=load_csv_to_postgres,
    )