import json
import os

from dotenv import load_dotenv
from psycopg2 import Error, connect
from psycopg2.extensions import connection

load_dotenv()


def connect_to_db() -> connection:
    """Возвращает подключение к базе данных PostgreSQL"""
    return connect(
        dbname=os.getenv('DB_NAME', default="postgres"),
        user=os.getenv('DB_USER', default="admin"),
        password=os.getenv('DB_PASSWORD', default="admin"),
        host=os.getenv('DB_HOST', default="localhost"),
        port=os.getenv('DB_PORT', default="5432")
    )


def create_table(conn: connection):
    """Создание таблицы в бд, если она не существует."""
    with conn.cursor() as cursor:
        try:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS organizations (
                    id SERIAL PRIMARY KEY,
                    ParentId INT,
                    Name TEXT NOT NULL,
                    Type INT NOT NULL
                );
                """
            )
            print('Таблица успешно создана')
            conn.commit()
        except Error as e:
            print(f"Ошибка при создании таблицы. {e}")
            conn.rollback()
            raise e


def import_data_from_json(json_file: str, conn: connection):
    """Импорт данных из JSON в таблицу organizations"""
    with open(json_file, "r", encoding="utf-8") as file:
        try:
            data = json.load(file)
        except Exception as e:
            raise e

    with conn.cursor() as cursor:
        for record in data:
            try:
                cursor.execute(
                    """
                    INSERT INTO organizations (id, ParentId, Name, Type)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (id) DO UPDATE
                    SET ParentId = excluded.ParentId,
                        Name = excluded.Name,
                        Type = excluded.Type;
                    """,
                    (record["id"], record["ParentId"],
                     record["Name"], record["Type"]),
                )
            except Error as e:
                conn.rollback()
                raise e
    print('Данные успешно загружены')
    conn.commit()


def main():
    try:
        with connect_to_db() as conn:
            create_table(conn)
            import_data_from_json("data.json", conn)
    except Exception as e:
        print(f"Произошла ошбика.{e}")


if __name__ == "__main__":
    main()
