from typing import List

from psycopg2 import Error
from psycopg2.errors import UndefinedTable
from psycopg2.extensions import connection

from db_init import connect_to_db


def get_employees_by_object(object_id: int, conn: connection) -> List[str]:
    """Получение всех сотрудников в офисе по идентификатору объекта"""
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                WITH RECURSIVE hierarchy AS (
                    SELECT id, ParentId, Name, Type
                    FROM organizations
                    WHERE id = %s
                    UNION ALL
                    SELECT o.id, o.ParentId, o.Name, o.Type
                    FROM organizations o
                    INNER JOIN hierarchy h ON o.id = h.ParentId
                ),
                office AS (
                    SELECT id
                    FROM hierarchy
                    WHERE Type = 1
                    LIMIT 1
                ),
                employees AS (
                    SELECT id, ParentId, Name, Type
                    FROM organizations
                    WHERE id = (SELECT id FROM office)
                    UNION ALL
                    SELECT o.id, o.ParentId, o.Name, o.Type
                    FROM organizations o
                    INNER JOIN employees e ON o.ParentId = e.id
                )
                SELECT Name
                FROM employees
                WHERE Type = 3;
                """,
                (object_id,)
            )
            employees = cursor.fetchall()
            return [employee[0] for employee in employees]
    except Error as e:
        raise e


def main(employee_id: int):
    try:
        with connect_to_db() as conn:
            employees = get_employees_by_object(employee_id, conn)
            if employees:
                print("Сотрудники в офисе:", ", ".join(employees))
            else:
                print("Сотрудники не найдены или офис отсутствует.")
    except UndefinedTable:
        print("Таблица не создана. Инициализируйте БД")
    except Exception as e:
        print(f"Произошла ошбика.\n {e}")


if __name__ == "__main__":
    employee_id = int(input('Введите идентификатор сотрудника: '))
    main(employee_id)
