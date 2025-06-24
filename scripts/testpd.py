import psycopg2
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

DB_PARAMS = {
    "database": os.getenv("DB_NAME2"),
    "user": os.getenv("DB_USER2"),
    "password": os.getenv("DB_PASSWORD2"),
    "host": os.getenv("DB_HOST2"),
    "port": os.getenv("DB_PORT2")
}

CSV_FILE = 'data/md_exchange_rate_d.csv'

def create_table(conn):
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS MD_EXCHANGE_RATE_D (
        data_actual_date DATE NOT NULL,
        data_actual_end_date DATE,
        currency_rk NUMERIC NOT NULL,
        reduced_cource FLOAT,
        code_iso_num VARCHAR(3),
        UNIQUE (data_actual_date, currency_rk)
    );
    """
    with conn.cursor() as cursor:
        cursor.execute(create_table_sql)
    conn.commit()

def load_data_from_csv(conn, csv_file):
    df = pd.read_csv(csv_file, sep=';')
    
    data = [tuple(x) for x in df.to_numpy()]
    
    insert_sql = """
    INSERT INTO MD_EXCHANGE_RATE_D (data_actual_date, data_actual_end_date, 
                            currency_rk, reduced_cource, code_iso_num)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (data_actual_date, currency_rk) 
    DO UPDATE SET 
        data_actual_end_date = EXCLUDED.data_actual_end_date,
        reduced_cource = EXCLUDED.reduced_cource,
        code_iso_num = EXCLUDED.code_iso_num;
    """
    
    with conn.cursor() as cursor:
        cursor.executemany(insert_sql, data)
    conn.commit()
    print(f"Загружено {len(data)} записей")

def main():
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        
        create_table(conn)
        
        load_data_from_csv(conn, CSV_FILE)
        
    except Exception as e:
        print(f"Ошибка: {e}")
    finally:
        if conn:
            conn.close()
            print("Соединение с PostgreSQL закрыто")

if __name__ == "__main__":
    main()