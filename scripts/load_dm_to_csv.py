from db_parameters import DB_PARAMS

from datetime import datetime
import psycopg2
import pandas as pd
from psycopg2.extras import RealDictCursor
from decimal import Decimal 

OUTPUT_FILE = 'data/dm_f101_round_f_export.csv'
QUERY = "SELECT * FROM dm.dm_f101_round_f"

def export_to_csv():
    conn = None
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        
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
            
            df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8')
        
        print(f"Экспортировано {len(df)} строк")
        
    except Exception as e:
        print(f"Ошибка: {str(e)}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    start_time = datetime.now()
    print(f"Начало выгрузки: {start_time}")
    
    export_to_csv()
    
    end_time = datetime.now()
    print(f"Выгрузка завершена: {end_time}")
    print(f"Общее время выполнения: {end_time - start_time}")