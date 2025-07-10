from db_parameters import DB_PARAMS, LOGS_DB_PARAMS

from decimal import Decimal 
from datetime import datetime, timezone, timedelta
from log_to_db import log_operation
from psycopg2.extras import RealDictCursor
import pandas as pd
import psycopg2
import time
import os

OUTPUT_FILE = 'data/dm_f101_round_f.csv'
QUERY = "SELECT * FROM dm.dm_f101_round_f"

def export_to_csv(conn, logs_conn):
    start_time = datetime.now(timezone(timedelta(hours=3)))
    inserted_count = 0  
    updated_count = 0  
    status = "SUCCESS"
    error_message = None
    total_written = 0
    time.sleep(5)

    try:    
        # Используем DictCursor для правильного определения типов
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(QUERY)
            data = cursor.fetchall()
            
            # Конвертируем в DataFrame
            df = pd.DataFrame(data)
            
            # Обрабатываем Decimal колонки
            for col in df.columns:
                if any(isinstance(x, Decimal) for x in df[col]):
                    df[col] = df[col].apply(
                        lambda x: '{0:.8f}'.format(x).rstrip('0').rstrip('.') 
                        if x != 0 else '0'
                    )
            
            df.to_csv(OUTPUT_FILE, sep=';', index=False, encoding='utf-8')

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
                os.path.basename(OUTPUT_FILE), 
                len(df))
        
    print(f"Экспортировано {len(df)} строк")
        

def main():
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        logs_conn = psycopg2.connect(**LOGS_DB_PARAMS)

        export_to_csv(conn, logs_conn)

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