import psycopg2
from settings import DBConfig, DBClickHouseConfig
from clickhouse_driver import Client

def get_db_connection():
    """
    Устанавливает соединение с базой данных PostgreSQL.
    Возвращает объект соединения и курсор.
    """
    try:
        # Подключение к базе данных
        conn = psycopg2.connect(
            host=DBConfig.DB_HOST,
            database=DBConfig.DB_NAME,
            user=DBConfig.DB_USER,
            password=DBConfig.DB_PASSWORD
        )
        cursor = conn.cursor()
        print("Подключение к базе данных успешно установлено!")
        return conn, cursor

    except Exception as e:
        print(f"Произошла ошибка при подключении к базе данных: {e}")
        return None, None


def get_clickhouse_connection():
    """
    Устанавливает соединение с базой данных ClickHouse.
    Возвращает объект клиента.
    """
    try:
        client = Client(
            host=DBClickHouseConfig.CLICKHOUSE_HOST,
            port=DBClickHouseConfig.CLICKHOUSE_PORT,
            user=DBClickHouseConfig.CLICKHOUSE_USER,
            password=DBClickHouseConfig.CLICKHOUSE_PASSWORD,
            database=DBClickHouseConfig.CLICKHOUSE_DB
        )
        client.execute('SELECT 1')
        print("Подключение к ClickHouse успешно установлено!")
        return client
    except Exception as e:
        print(f"Произошла ошибка при подключении к ClickHouse: {e}")
        return None
