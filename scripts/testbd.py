import psycopg2
from psycopg2 import OperationalError
import pandas as pd
import sys
import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

DB_PARAMS = {
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT")
}

CSV_FILE = 'data/ft_balance_f.csv'


def load_data_from_csv(csv_file):
    df = pd.read_csv(csv_file, sep=';')
    df['ON_DATE'] = pd.to_datetime(df['ON_DATE'], format='%d.%m.%Y').dt.strftime('%Y-%m-%d')
    data = [tuple(x) for x in df.to_numpy()]
    print(data)



def write_to_bd(conn):
    insert_sql = """
    INSERT INTO t_table (log)
    VALUES ('hello');
    """
    with conn.cursor() as cursor:
        cursor.execute(insert_sql)
    conn.commit()
    print("Запись загружена")

def test_connection(conn):
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        print("Успешное подключение к PostgreSQL!")
        conn.close()
    except OperationalError as e:
        print(f"Ошибка подключения: {e}")

def main():
    try:
        # Установка соединения с базой данных
        conn = psycopg2.connect(**DB_PARAMS)
        print("Успешное подключение к PostgreSQL!")

        # Создание таблицы
        write_to_bd(conn)

        load_data_from_csv(CSV_FILE)
              
    except Exception as e:
        print(f"Ошибка: {e}")
    finally:
        if conn:
            conn.close()
            print("Соединение с PostgreSQL закрыто")


if __name__ == "__main__":
    main()