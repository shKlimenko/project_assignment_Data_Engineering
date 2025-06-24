import psycopg2
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

LOGS_DB_PARAMS = {
    "database": os.getenv("LOGS_DB_NAME"),
    "user": os.getenv("LOGS_DB_USER"),
    "password": os.getenv("LOGS_DB_PASSWORD"),
    "host": os.getenv("LOGS_DB_HOST"),
    "port": os.getenv("LOGS_DB_PORT")
}


def log_operation(conn, start_time, end_time, status, error_message, file_name, record_count):
    """Записывает информацию о выполнении операции в лог"""
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
            INSERT INTO logs.data_load_log 
                (start_time, end_time, status, error_message, file_name, record_count)
            VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                start_time,
                end_time,
                status,
                error_message,
                file_name,
                record_count
            ))
        conn.commit()
    except Exception as e:
        print(f"Ошибка при записи лога: {e}")
        conn.rollback()
        raise




'''def log_operation(conn, start_time, end_time, status, error_message, file_name, record_count):
    """Записывает информацию о выполнении операции в лог"""
    
    insert_sql = """
        INSERT INTO logs.data_load_log 
            (start_time, end_time, status, error_message, file_name, record_count)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
    
    with conn.cursor() as cursor:
        cursor.executemany(insert_sql, (start_time, end_time, status, error_message, file_name, record_count))
    conn.commit()'''

