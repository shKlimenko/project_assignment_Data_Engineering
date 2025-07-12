import psycopg2
import pandas as pd
import os
from log_to_db import log_operation
from datetime import datetime, timezone, timedelta
from db_parameters import DB_PARAMS, LOGS_DB_PARAMS
import time

CSV_FILE = 'data/dm_f101_round_f.csv'

def create_table(conn):
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS DM.DM_F101_ROUND_F_v2 (
        FROM_DATE DATE,
        TO_DATE DATE,
        CHAPTER CHAR(1),
        LEDGER_ACCOUNT CHAR(5),
        CHARACTERISTIC CHAR(1),
        BALANCE_IN_RUB NUMERIC(23, 8),
        BALANCE_IN_VAL NUMERIC(23, 8),
        BALANCE_IN_TOTAL NUMERIC(23, 8),
        TURN_DEB_RUB NUMERIC(23, 8),
        TURN_DEB_VAL NUMERIC(23, 8),
        TURN_DEB_TOTAL NUMERIC(23, 8),
        TURN_CRE_RUB NUMERIC(23, 8),
        TURN_CRE_VAL NUMERIC(23, 8),
        TURN_CRE_TOTAL NUMERIC(23, 8),
        BALANCE_OUT_RUB NUMERIC(23, 8),
        BALANCE_OUT_VAL NUMERIC(23, 8),
        BALANCE_OUT_TOTAL NUMERIC(23, 8)
    );
    TRUNCATE TABLE DM.DM_F101_ROUND_F_v2;
    """
    with conn.cursor() as cursor:
        cursor.execute(create_table_sql)
    conn.commit()

def load_data_from_csv(conn, logs_conn, csv_file):
    start_time = datetime.now(timezone(timedelta(hours=3)))
    status = "SUCCESS"
    error_message = None
    time.sleep(5)

    try:
        df = pd.read_csv(csv_file, sep=';')
    
        data = [tuple(x) for x in df.to_numpy()]
    
        insert_sql = """
        INSERT INTO DM.DM_F101_ROUND_F_v2 (FROM_DATE, TO_DATE, CHAPTER, LEDGER_ACCOUNT, CHARACTERISTIC,
                                BALANCE_IN_RUB, BALANCE_IN_VAL, BALANCE_IN_TOTAL,
                                TURN_DEB_RUB, TURN_DEB_VAL, TURN_DEB_TOTAL,
                                TURN_CRE_RUB, TURN_CRE_VAL, TURN_CRE_TOTAL,
                                BALANCE_OUT_RUB, BALANCE_OUT_VAL, BALANCE_OUT_TOTAL)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING (xmax = 0) AS inserted;
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