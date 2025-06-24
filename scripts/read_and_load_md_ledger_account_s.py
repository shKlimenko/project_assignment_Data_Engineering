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

CSV_FILE = 'data/md_ledger_account_s.csv'

def create_table(conn):
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS MD_LEDGER_ACCOUNT_S (
        chapter CHAR(1),
        chapter_name VARCHAR(16),
        section_number INTEGER,
        section_name VARCHAR(22),
        subsection_name VARCHAR(21),
        ledger1_account INTEGER,
        ledger1_account_name VARCHAR(47),
        ledger_account INTEGER not NULL,
        ledger_account_name VARCHAR(153),
        characteristic CHAR(1),
        is_resident INTEGER,
        is_reserve INTEGER,
        is_reserved INTEGER,
        is_loan INTEGER,
        is_reserved_assets INTEGER,
        is_overdue INTEGER,
        is_interest INTEGER,
        pair_account VARCHAR(5),
        start_date DATE not NULL,
        end_date DATE,
        is_rub_only INTEGER,
        min_term VARCHAR(1),
        min_term_measure VARCHAR(1),
        max_term VARCHAR(1),
        max_term_measure VARCHAR(1),
        ledger_acc_full_name_translit VARCHAR(1),
        is_revaluation VARCHAR(1),
        is_correct VARCHAR(1),
        UNIQUE (ledger_account, start_date)
    );
    """
    with conn.cursor() as cursor:
        cursor.execute(create_table_sql)
    conn.commit()

def load_data_from_csv(conn, csv_file):
    df = pd.read_csv(csv_file, sep=';', dtype={
                                            'CHAPTER': 'str',               # CHAR(1)
                                            'CHAPTER_NAME': 'str',          # VARCHAR(16)
                                            'SECTION_NUMBER': 'Int64',      # INTEGER (поддержка NULL)
                                            'SECTION_NAME': 'str',          # VARCHAR(22)
                                            'SUBSECTION_NAME': 'str',       # VARCHAR(21)
                                            'LEDGER1_ACCOUNT': 'Int64',     # INTEGER
                                            'LEDGER1_ACCOUNT_NAME': 'str',  # VARCHAR(47)
                                            'LEDGER_ACCOUNT': 'Int64',      # INTEGER NOT NULL
                                            'LEDGER_ACCOUNT_NAME': 'str',   # VARCHAR(153)
                                            'CHARACTERISTIC': 'str',        # CHAR(1)
                                            'START_DATE': 'str',            # DATE (преобразуем позже)
                                            'END_DATE': 'str'               # DATE (преобразуем позже)
                                        })
    df['START_DATE'] = pd.to_datetime(df['START_DATE']).dt.date
    df['END_DATE'] = pd.to_datetime(df['END_DATE']).dt.date

    data = [tuple(x) for x in df.to_numpy()]
    
    insert_sql = """
    INSERT INTO MD_LEDGER_ACCOUNT_S (chapter, chapter_name, section_number, section_name ,
                                subsection_name, ledger1_account, ledger1_account_name , 
                                ledger_account, ledger_account_name, characteristic,
                                start_date, end_date)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (ledger_account, start_date) 
    DO UPDATE SET 
        chapter = EXCLUDED.chapter,
        chapter_name = EXCLUDED.chapter_name,
        section_number = EXCLUDED.section_number,
        section_name = EXCLUDED.section_name,
        subsection_name = EXCLUDED.subsection_name,
        ledger1_account = EXCLUDED.ledger1_account,
        ledger1_account_name = EXCLUDED.ledger1_account_name,
        ledger_account_name = EXCLUDED.ledger_account_name,
        characteristic = EXCLUDED.characteristic,
        end_date = EXCLUDED.end_date;
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