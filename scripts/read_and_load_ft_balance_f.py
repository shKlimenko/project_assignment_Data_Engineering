import psycopg2
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

DB_PARAMS = {
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT")
}

CSV_FILE = 'data/ft_balance_f.csv'

def create_table(conn):
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS FT_BALANCE_F (
        on_date DATE NOT NULL,
        account_rk NUMERIC NOT NULL,
        currency_rk NUMERIC,
        balance_out FLOAT,
        UNIQUE (on_date, account_rk)
    );
    """
    with conn.cursor() as cursor:
        cursor.execute(create_table_sql)
    conn.commit()

def load_data_from_csv(conn, csv_file):
    df = pd.read_csv(csv_file, sep=';')
    
    # Преобразование даты из формата DD.MM.YYYY в YYYY-MM-DD
    df['ON_DATE'] = pd.to_datetime(df['ON_DATE'], format='%d.%m.%Y').dt.strftime('%Y-%m-%d')
    
    data = [tuple(x) for x in df.to_numpy()]
    
    insert_sql = """
    INSERT INTO FT_BALANCE_F (on_date, account_rk, currency_rk, balance_out)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (on_date, account_rk) 
    DO UPDATE SET 
        currency_rk = EXCLUDED.currency_rk,
        balance_out = EXCLUDED.balance_out;
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