import psycopg2

from src.db_manager import DBManager


def choose_one_of(question: str, *args: str):
    """
    задаёт вопрос и просит выбрать один из вариантов ответа
    :param question: задаваемый вопрос
    :param args: варианты ответа
    :return: выбранный вариант
    """
    while True:
        print(question)
        i = 0
        for item in args:
            i += 1
            print(f"{i}. {item}")
        user_input = input()
        i = 0
        for item in args:
            i += 1
            if user_input.lower() in [str(i), item.lower()]:
                return args[i - 1]
        print("Sorry, I don't understand you. Let's try again.")

def print_vacancy(vacancy):
    print(f"{vacancy[0]}. {vacancy[1]}")
    print(f"URL: {vacancy[2]}")
    match bool(vacancy[3]), bool(vacancy[4]):
        case True, True:
            print(f"Зарплата от {vacancy[3]} до {vacancy[4]}")
        case True, False:
            print(f"Зарплата от {vacancy[3]}")
        case False, True:
            print(f"Зарплата до {vacancy[4]}")
        case False, False:
            print(f"Зарплата не указана")
    print()



if __name__ == '__main__':
    searching_company = input("Введите интересующую Вас компанию: ")

    conn = psycopg2.connect(
        host='localhost',
        database='hh_vacancies',
        user='postgres',
        password='mihanik95'
    )
    try:
        db_manager = DBManager(searching_company, conn)
        print(f"Работаем с вакансиями компании {db_manager.name}")
        while True:
            print()
            match choose_one_of("Выберите варианты действий:",
                                "Получить информацию о вакансиях",
                                "Получить среднюю зарплату по вакансиям",
                                "Получить список вакансий с минимальной зарплатой выше средней",
                                "Получить вакансии, содержащие ключевое слово",
                                "Выйти из программы"):
                case "Получить информацию о вакансиях":
                    for item in db_manager.get_all_vacancies():
                        print_vacancy(item)
                case "Получить среднюю зарплату по вакансиям":
                    print(f"{db_manager.get_avg_salary()} руб.")
                case "Получить список вакансий с минимальной зарплатой выше средней":
                    for item in db_manager.get_vacancies_with_higher_minimal_salary():
                        print_vacancy(item)
                case "Получить вакансии, содержащие ключевое слово":
                    searching_list = db_manager.get_vacancies_with_keyword(input("Введите ключевое слово: "))
                    if not searching_list:
                        print("Нет вакансий с данным ключевым словом")
                    else:
                        for item in searching_list:
                            print_vacancy(item)
                case "Выйти из программы":
                    break
    finally:
        conn.close()
