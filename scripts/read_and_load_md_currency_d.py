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

CSV_FILE = 'data/md_currency_d.csv'

def create_table(conn):
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS MD_CURRENCY_D (
        currency_rk NUMERIC NOT NULL,
        data_actual_date DATE NOT NULL,
        data_actual_end_date DATE,
        currency_code VARCHAR(3),
        code_iso_char VARCHAR(3),
        UNIQUE (currency_rk, data_actual_date)
    );
    """
    with conn.cursor() as cursor:
        cursor.execute(create_table_sql)
    conn.commit()

def load_data_from_csv(conn, csv_file):
    with open(csv_file, 'r', encoding='cp1252') as f:
        df = pd.read_csv(f, sep=';')
    
    # необходимо CURRENCY_CODE привести к целочисленному значению, или тексту. чтобы 356 отображалось как 356 а не 356.0

    # Проверка длин значений
    print("Максимальная длина currency_code:", df['CURRENCY_CODE'].astype(str).str.len().max())
    print("Максимальная длина code_iso_char:", df['CODE_ISO_CHAR'].astype(str).str.len().max())
    
    # Вывод строк, где длина превышает 3 символа
    print("Проблемные строки по currency_code:")
    print(df[df['CURRENCY_CODE'].astype(str).str.len() > 3])
    print("Проблемные строки по code_iso_char:")
    print(df[df['CODE_ISO_CHAR'].astype(str).str.len() > 3])

    data = [tuple(x) for x in df.to_numpy()]
    
    insert_sql = """
    INSERT INTO MD_CURRENCY_D (currency_rk, data_actual_date, data_actual_end_date, currency_code, code_iso_char)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (currency_rk, data_actual_date) 
    DO UPDATE SET 
        data_actual_end_date = EXCLUDED.data_actual_end_date,
        currency_code = EXCLUDED.currency_code,
        code_iso_char = EXCLUDED.code_iso_char;
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