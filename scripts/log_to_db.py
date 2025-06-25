def create_table_if_not_exists(conn):
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS data_load_log (
        id SERIAL PRIMARY KEY,
        start_time TIMESTAMP NOT NULL,
        end_time TIMESTAMP NOT NULL,
        status VARCHAR(20) NOT NULL,
        error_message TEXT,
        file_name VARCHAR(255) NOT NULL,
        record_count INT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    with conn.cursor() as cursor:
        cursor.execute(create_table_sql)
    conn.commit()

def log_operation(conn, start_time, end_time, status, error_message, file_name, record_count):
    create_table_if_not_exists(conn)
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