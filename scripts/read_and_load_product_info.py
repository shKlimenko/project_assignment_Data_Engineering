import psycopg2
import pandas as pd
import os
from log_to_db import log_operation
from datetime import datetime, timezone, timedelta
from db_parameters import DB_PARAMS_LOC, LOGS_DB_PARAMS
import time


CSV_FILE = 'data/loan_holiday_info/product_info.csv'

def load_data_from_csv(conn, logs_conn, csv_file):
    start_time = datetime.now(timezone(timedelta(hours=3)))
    status = "SUCCESS"
    error_message = None
    time.sleep(5)

    try:
        encodings_to_try = ['utf-8', 'windows-1251', 'cp1252', 'iso-8859-1']
        
        for encoding in encodings_to_try:
            try:
                df = pd.read_csv(csv_file, sep=',', encoding=encoding)
                break
            except UnicodeDecodeError:
                continue
        else:
            raise ValueError("Не удалось определить кодировку файла")
        
        df = df.where(pd.notnull(df), None)
        data = [tuple(x) for x in df.to_numpy()]
        
        truncate_sql = """
        TRUNCATE TABLE rd.product;
        """

        insert_sql = """
        INSERT INTO rd.product 
            (product_rk, product_name, effective_from_date, effective_to_date)
        VALUES (%s, %s, %s, %s)
        RETURNING (xmax = 0) AS inserted;
        """
        
        with conn.cursor() as cursor:
            cursor.execute(truncate_sql)
            cursor.executemany(insert_sql, data)
        conn.commit()
        
    except Exception as e:
        conn.rollback()
        status = "FAILED"
        error_message = str(e)
        raise
    finally:
        end_time = datetime.now(timezone(timedelta(hours=3)))
        log_operation(
                logs_conn, 
                start_time.strftime('%Y-%m-%d %H:%M:%S'), 
                end_time.strftime('%Y-%m-%d %H:%M:%S'), 
                status, 
                error_message, 
                os.path.basename(csv_file), 
                len(df))
        
    print(f"Обработано строк: {len(df)})")

def main():
    try:
        conn = psycopg2.connect(**DB_PARAMS_LOC)
        logs_conn = psycopg2.connect(**LOGS_DB_PARAMS)
                
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