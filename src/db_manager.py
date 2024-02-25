import psycopg2
import requests


class DBManager:

    def __init__(self, company):
        # запрашиваем список работодателей по названию компании
        params = {'text': company}
        response = requests.get(url='https://api.hh.ru/employers', params=params).json()['items']

        # выбираем работодателя с наибольшим ассортиментом вакансий
        the_biggest_employer = response[1]
        for vacancy in response:
            if vacancy["open_vacancies"] > the_biggest_employer["open_vacancies"]:
                the_biggest_employer = vacancy

        # сохраняем данные выбранного работодателя в объект
        self.name = the_biggest_employer['name']
        self.id = the_biggest_employer['id']
        self.url = the_biggest_employer['url']
        self.vacancies_url = the_biggest_employer['vacancies_url']
        self.vacancies = requests.get(url=self.vacancies_url).json()['items']

        # начинаем работу с БД
        conn = psycopg2.connect(
            host='localhost',
            database='hh_vacancies',
            user='postgres',
            password='mihanik95'
        )

        try:
            with conn:
                with conn.cursor() as cur:
                    # проверяем таблицы БД hh_vacancies на существование
                    cur.execute("""CREATE TABLE IF NOT EXISTS employers(
                                                    id int PRIMARY KEY,
                                                    employer_name varchar,
                                                    url varchar,
                                                    vacancies_url varchar
                                                )""")
                    cur.execute("""CREATE TABLE IF NOT EXISTS vacancies(
                                                    id int PRIMARY KEY,
                                                    vacancy_name varchar,
                                                    url varchar,
                                                    salary_from int,
                                                    salary_to int,
                                                    employer_id int,
                                                    
                                                    CONSTRAINT vacancy_employer_id FOREIGN KEY (employer_id) 
                                                    REFERENCES employers(id)
                                                )""")

                    # добавляем данные о работодателе и вакансиях в таблицы
                    try:
                        cur.execute("""INSERT INTO employers VALUES (%s, %s, %s, %s)""",
                                    (self.id, self.name, self.url, self.vacancies_url))
                    except psycopg2.errors.UniqueViolation:
                        conn.rollback()

                    for item in self.vacancies:
                        try:
                            cur.execute("INSERT INTO vacancies VALUES (%s, %s, %s, %s, %s, %s)",
                                        (item['id'],
                                         item['name'],
                                         item['url'],
                                         0 if item['salary'] is None else int(item['salary']['from'] or 0),
                                         0 if item['salary'] is None else int(item['salary']['to'] or 0),
                                         self.id))
                        except psycopg2.errors.UniqueViolation:
                            conn.rollback()
        finally:
            conn.close()

    def get_companies_and_vacancies_count(self):
        conn = psycopg2.connect(
            host='localhost',
            database='hh_vacancies',
            user='postgres',
            password='mihanik95'
        )

        try:
            with conn:
                with conn.cursor() as cur:
                    pass
        finally:
            conn.close()

    def get_all_vacancies(self):
        conn = psycopg2.connect(
            host='localhost',
            database='hh_vacancies',
            user='postgres',
            password='mihanik95'
        )

        try:
            with conn:
                with conn.cursor() as cur:
                    pass
        finally:
            conn.close()

    def get_avg_salary(self):
        conn = psycopg2.connect(
            host='localhost',
            database='hh_vacancies',
            user='postgres',
            password='mihanik95'
        )

        try:
            with conn:
                with conn.cursor() as cur:
                    pass
        finally:
            conn.close()

    def get_vacancies_with_higher_salary(self):
        conn = psycopg2.connect(
            host='localhost',
            database='hh_vacancies',
            user='postgres',
            password='mihanik95'
        )

        try:
            with conn:
                with conn.cursor() as cur:
                    pass
        finally:
            conn.close()

    def get_vacancies_with_keyword(self, keyword):
        conn = psycopg2.connect(
            host='localhost',
            database='hh_vacancies',
            user='postgres',
            password='mihanik95'
        )

        try:
            with conn:
                with conn.cursor() as cur:
                    pass
        finally:
            conn.close()
