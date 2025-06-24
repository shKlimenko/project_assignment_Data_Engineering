import psycopg2
import pandas as pd
import os
from log_to_db import log_operation
from datetime import datetime
from db_parameters import DB_PARAMS, LOGS_DB_PARAMS


CSV_FILE = 'data/ft_posting_f.csv'

def create_table(conn):
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS FT_POSTING_F (
	    oper_date DATE NOT NULL,
	    credit_account_rk NUMERIC NOT NULL,
	    debet_account_rk NUMERIC NOT NULL,
	    credit_amount FLOAT,
	    debet_amount FLOAT
    );
    """
    with conn.cursor() as cursor:
        cursor.execute(create_table_sql)
    conn.commit()

def load_data_from_csv(conn, logs_conn, csv_file):
    start_time = datetime.now()
    inserted_count = 0  
    updated_count = 0  
    status = "SUCCESS"
    error_message = None

    try:
        df = pd.read_csv(csv_file, sep=';')
    
        # Преобразование даты из формата DD.MM.YYYY в YYYY-MM-DD
        df['OPER_DATE'] = pd.to_datetime(df['OPER_DATE'], format='%d-%m-%Y').dt.strftime('%Y-%m-%d')
        
        data = [tuple(x) for x in df.to_numpy()]
        
        insert_sql = """
        INSERT INTO FT_POSTING_F (oper_date, credit_account_rk, debet_account_rk, credit_amount, debet_amount)
        VALUES (%s, %s, %s, %s, %s)
        RETURNING (xmax = 0) AS inserted;
        """
        
        with conn.cursor() as cursor:
            for row in data:
                cursor.execute(insert_sql, row)
                result = cursor.fetchone()
                if result[0]:
                    inserted_count += 1
                else:
                    updated_count += 1
        conn.commit()
        total_written = inserted_count + updated_count
        
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
                total_written)
        
    print(f"Обработано записей: {total_written} (вставлено: {inserted_count}, обновлено: {updated_count})")

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