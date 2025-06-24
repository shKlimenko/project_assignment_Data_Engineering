import psycopg2
import pandas as pd
import os
from dotenv import load_dotenv
from log_to_db import *
from datetime import datetime

load_dotenv()

DB_PARAMS = {
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT")
}

CSV_FILE = 'data/md_account_d.csv'

def create_table(conn):
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS MD_ACCOUNT_D (
        data_actual_date DATE NOT NULL,
        data_actual_end_date DATE NOT NULL,
        account_rk NUMERIC NOT NULL,
        account_number VARCHAR(20) NOT NULL,
        char_type VARCHAR(1) NOT NULL,
        currency_rk NUMERIC NOT NULL,
        currency_code VARCHAR(3) NOT NULL,
        UNIQUE (data_actual_date, account_rk)
    );
    """
    with conn.cursor() as cursor:
        cursor.execute(create_table_sql)
    conn.commit()

def load_data_from_csv(conn, logs_conn, csv_file):
    """Загружает данные из CSV в основную таблицу"""
    start_time = datetime.now()
    record_count = 0
    status = "SUCCESS"
    error_message = None


    try:
        df = pd.read_csv(csv_file, sep=';')
        data = [tuple(x) for x in df.to_numpy()]
        record_count = len(data)
        
        insert_sql = """
        INSERT INTO MD_ACCOUNT_D 
            (data_actual_date, data_actual_end_date, account_rk, 
             account_number, char_type, currency_rk, currency_code)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (data_actual_date, account_rk) 
        DO UPDATE SET 
            data_actual_end_date = EXCLUDED.data_actual_end_date,
            account_number = EXCLUDED.account_number,
            char_type = EXCLUDED.char_type,
            currency_rk = EXCLUDED.currency_rk,
            currency_code = EXCLUDED.currency_code;
        """
        
        with conn.cursor() as cursor:
            cursor.executemany(insert_sql, data)
        conn.commit()
        
    except Exception as e:
        conn.rollback()
        status = "FAILED"
        error_message = str(e)
        raise
    finally:
        end_time = datetime.now()
        log_operation(
                logs_conn, 
                start_time.strftime('%Y-%m-%d %H:%M:%S'), 
                end_time.strftime('%Y-%m-%d %H:%M:%S'), 
                status, 
                error_message, 
                os.path.basename(csv_file), 
                record_count)
        
    print(f"Загружено {record_count} записей")

def main():
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        logs_conn = psycopg2.connect(**LOGS_DB_PARAMS)
        
        create_table(conn)
        
        load_data_from_csv(conn, logs_conn, CSV_FILE)
        
    except Exception as e:
        print(f"Ошибка: {e}")
    finally:
        if conn:
            conn.close()
        if logs_conn:
            logs_conn.close()
        print("Соединения с PostgreSQL закрыты")

if __name__ == "__main__":
    main()