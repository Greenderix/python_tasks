import csv
import os
import time
from db_app import get_db_connection

# Имя таблицы и файла
TABLE_NAME = 'big_table'
CSV_FILE_NAME = f'{TABLE_NAME}.csv'
# Размер чанка (количество строк за один запрос)
CHUNK_SIZE = 100000


def export_table_to_csv():
    conn, cursor = None, None
    try:
        # Устанавливаем соединение с базой данных
        conn, cursor = get_db_connection()
        if not conn or not cursor:
            raise Exception("Не удалось подключиться к базе данных.")

        # Проверка существования файла
        file_exists = os.path.isfile(CSV_FILE_NAME)

        # Если файл не существует, создаем его и записываем заголовки
        if not file_exists:
            with open(CSV_FILE_NAME, mode='w', newline='', encoding='utf-8') as csv_file:
                writer = csv.writer(csv_file)

                # Получение заголовков столбцов
                cursor.execute(f'''SELECT * FROM {TABLE_NAME} LIMIT 0''')
                column_names = [desc[0] for desc in cursor.description]
                writer.writerow(column_names)  # Запись заголовков в CSV

        # Выгрузка данных чанками
        offset = 0
        while True:
            # Замер времени и памяти перед чтением чанка
            start_read_time = time.time()
            cursor.execute(
                f'''SELECT * FROM {TABLE_NAME} ORDER BY id OFFSET %s LIMIT %s''',
                (offset, CHUNK_SIZE)
            )
            rows = cursor.fetchall()

            if not rows:
                break  # Если строк больше нет, завершаем цикл

            with open(CSV_FILE_NAME, mode='a', newline='', encoding='utf-8') as csv_file:
                writer = csv.writer(csv_file)
                writer.writerows(rows)  # Запись чанка в CSV

            offset += len(rows)
            print(
                f"Обработано {offset} строк"
            )

        print(f"Данные успешно выгружены в файл: {CSV_FILE_NAME}")

    except Exception as e:
        print(f"Произошла ошибка: {e}")

    finally:
        # Закрытие соединения с базой данных
        if conn:
            cursor.close()
            conn.close()
            print("Соединение с базой данных закрыто.")

# Запуск функции выгрузки
export_table_to_csv()