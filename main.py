from db_init import main as db_init
from employee import main as get_data


def main():
    while True:
        ask = input(
            'Выберите действие: Инициализировать БД(I), ' +
            'Получить выборку сотрудников(G), выйти(X):')
        if ask == 'I' or ask == 'i':
            db_init()
        elif ask == 'G' or ask == 'g':
            try:
                employee_id = int(input('Введите идентификатор сотрудника: '))
                get_data(employee_id)
                continue
            except ValueError:
                print('Необходимо ввести положительное, целое число')
        elif ask == '':
            continue
        elif ask == 'X' or ask == 'x':
            break
        else:
            print('Такого варианта нет')
            continue


if __name__ == "__main__":
    main()
