import psycopg2
import requests
from functools import wraps


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
        self.vacancies = requests.get(url=f"{self.vacancies_url}&per_page=100").json()['items']

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

                    # создаём таблицы employers и vacancies, если они не существуют
                    cur.execute("""
                                CREATE TABLE IF NOT EXISTS employers(
                                    id int PRIMARY KEY,
                                    employer_name varchar,
                                    url varchar,
                                    vacancies_url varchar
                                )""")
                    cur.execute("""
                                CREATE TABLE IF NOT EXISTS vacancies(
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

    @staticmethod
    def db_working(f):
        """
        декоратор для работы функций с БД
        """

        @wraps(f)
        def wrapper(*args, **kwargs):
            conn = psycopg2.connect(
                host='localhost',
                database='hh_vacancies',
                user='postgres',
                password='mihanik95'
            )

            try:
                with conn:
                    with conn.cursor() as cur:
                        return f(cur=cur, *args, **kwargs)
            finally:
                conn.close()

        return wrapper

    @staticmethod
    @db_working
    def get_companies_and_vacancies_count(cur):
        """
        счетает и возвращает количество всех компаний и их вакансий в БД
        """
        cur.execute("SELECT COUNT(id) FROM employers")
        result = cur.fetchall()[0][0]
        cur.execute("SELECT COUNT(id) FROM vacancies")
        result += cur.fetchall()[0][0]
        return result

    @db_working
    def get_all_vacancies(self, cur):
        """
        возвращает все вакансии данного работодателя
        """
        cur.execute(f"""SELECT * FROM vacancies
                    WHERE employer_id={self.id}""")
        return cur.fetchall()

    @db_working
    def get_avg_salary(self, cur):
        """
        считает и возвращает среднюю зарплату по вакансиям данного работодателя
        """
        cur.execute(f"""SELECT AVG((salary_to+salary_from)/2)
                    FROM vacancies
                    WHERE employer_id={self.id}""")
        return int(cur.fetchall()[0][0])

    @db_working
    def get_vacancies_with_higher_minimal_salary(self, cur):
        """
        возвращает вакансии с минимальной зарплатой выше средней среди всех вакансий
        данного работодателя
        """
        cur.execute(f"""
                    SELECT * FROM vacancies
                    WHERE salary_from>=(SELECT AVG(salary_from) FROM vacancies 
                    WHERE employer_id={self.id})
                    AND employer_id={self.id}
                    """)
        return cur.fetchall()

    @db_working
    def get_vacancies_with_keyword(self, keyword, cur):
        """
        возвращает вакансии данного работодателя, содержащие заданное
        ключевое слово
        """
        cur.execute(f"""SELECT * FROM vacancies
                    WHERE vacancy_name LIKE '%{keyword}%'
                    AND employer_id={self.id}""")
        return cur.fetchall()
