import psycopg2
import pandas as pd
import os
from log_to_db import log_operation
from datetime import datetime
from db_parameters import DB_PARAMS, LOGS_DB_PARAMS

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

def load_data_from_csv(conn, logs_conn, csv_file):
    start_time = datetime.now()
    inserted_count = 0  
    updated_count = 0  
    status = "SUCCESS"
    error_message = None

    try:
        df = pd.read_csv(csv_file, sep=';', dtype={
                                            'CHAPTER': 'str',
                                            'CHAPTER_NAME': 'str',
                                            'SECTION_NUMBER': 'Int64',
                                            'SECTION_NAME': 'str',
                                            'SUBSECTION_NAME': 'str',
                                            'LEDGER1_ACCOUNT': 'Int64',
                                            'LEDGER1_ACCOUNT_NAME': 'str',
                                            'LEDGER_ACCOUNT': 'Int64',
                                            'LEDGER_ACCOUNT_NAME': 'str',
                                            'CHARACTERISTIC': 'str',
                                            'START_DATE': 'str',
                                            'END_DATE': 'str'
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
            end_date = EXCLUDED.end_date
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