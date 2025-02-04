from db_app import get_clickhouse_connection, get_db_connection

TABLE_NAME = 'clients'
def transfer_data_chunked(pg_cursor, ch_client, CHUNK_SIZE=10000):
    try:
        # Получаем общее количество строк в таблице PostgreSQL
        pg_cursor.execute(f'''SELECT COUNT(*) FROM {TABLE_NAME}''')
        total_rows = pg_cursor.fetchone()[0]
        print(f"Всего строк для переноса: {total_rows}")

        # Определяем количество чанков
        num_chunks = (total_rows // CHUNK_SIZE) + (1 if total_rows % CHUNK_SIZE else 0)

        # Переносим данные чанками
        for chunk in range(num_chunks):
            offset = chunk * CHUNK_SIZE
            pg_cursor.execute(
                f'''SELECT * FROM big_table LIMIT {CHUNK_SIZE} OFFSET {offset}''',
                (offset, CHUNK_SIZE)
            )
            rows = pg_cursor.fetchall()

            # Вставляем данные в ClickHouse
            ch_client.execute(f'''INSERT INTO {TABLE_NAME} VALUES''', rows)
            print(f"Перенесено {len(rows)} строк (чанк {chunk + 1}/{num_chunks})")

        print("Перенос данных завершен успешно!")

    except Exception as e:
        print(f"Произошла ошибка при переносе данных: {e}")

# Основная функция
def main():
    # Получаем соединение с PostgreSQL
    pg_conn, pg_cursor = get_db_connection()
    if pg_conn is None or pg_cursor is None:
        raise Exception("Не удалось подключиться к PostgreSQL.")

    # Получаем соединение с ClickHouse
    ch_client = get_clickhouse_connection()
    if ch_client is None:

        raise Exception("Не удалось подключиться к ClickHouse.")

    # Переносим данные
    transfer_data_chunked(pg_cursor, ch_client)

    # Закрываем соединения
    pg_cursor.close()
    pg_conn.close()
    ch_client.disconnect()

main()
