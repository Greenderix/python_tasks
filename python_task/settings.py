from os import environ


class DBConfig:
    DB_HOST = environ.get('DB_HOST')
    DB_NAME = environ.get('DB_NAME')
    DB_USER = environ.get('DB_USER')
    DB_PASSWORD = environ.get('DB_PASSWORD')


class DBClickHouseConfig:
    CLICKHOUSE_HOST = environ.get('CLICKHOUSE_HOST')
    CLICKHOUSE_PORT = environ.get('CLICKHOUSE_PORT')
    CLICKHOUSE_USER = environ.get('CLICKHOUSE_USER')
    CLICKHOUSE_PASSWORD = environ.get('CLICKHOUSE_PASSWORD')
    CLICKHOUSE_DB = environ.get('CLICKHOUSE_DB')
