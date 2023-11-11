import mysql.connector
import pandas as pd
from mysql.connector import Error

def connectionDB(server, login, password, database=None):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=server,
            user=login,
            password=password,
            database=database
        )
        print("З'єднання з базою даних встановлено!")
    except Error as e:
        print(f"Помилка при з'єднанні з базою даних: {e}")
    return connection

def create_DB(connection):
    try:
        cursor = connection.cursor()
        database_name = "University"
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")
        print(f"База даних '{database_name}' створена")
    except Error as e:
        print(f"Помилка при створенні бази даних: {e}")

def create_tables(connection):
    try:
        database_name = "University"
        cursor = connection.cursor()
        cursor.execute(f"USE {database_name}")

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Students (
                ID INT PRIMARY KEY,
                Surname VARCHAR(30),
                Name VARCHAR(30),
                Patronymic VARCHAR(30),
                Address VARCHAR(50),
                Phone VARCHAR(10),
                Course INT CHECK (Course BETWEEN 1 AND 4),
                Faculty VARCHAR(50),
                Grup VARCHAR(10),
                Headman BOOLEAN
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Subjects (
                ID INT PRIMARY KEY,
                Name VARCHAR(50),
                Hours INT,
                Semesters INT
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Exam (
                ID INT PRIMARY KEY,
                Date DATE,
                ID_student INT,
                ID_subject INT,
                Grade INT CHECK (Grade BETWEEN 2 AND 5),
                FOREIGN KEY (ID_student) REFERENCES Students(ID),
                FOREIGN KEY (ID_subject) REFERENCES Subjects(ID)
            )
        """)

        connection.commit()
        print("Таблиці створено!")
    except Error as e:
        print(f"Помилка при створенні таблиць: {e}")


def query_insert_for_tables(connection):

    query = (f"""
        INSERT INTO Students (ID ,Surname, Name, Patronymic, Address, Phone, Course, Faculty, Grup, Headman)
        VALUES
        (1, 'Іванов', 'Іван', 'Андрійович', 'вул. Шевченка, 1', '0636434671', 3, 'Аграрного менеджменту', 'A-301', TRUE),
        (2, 'Петров', 'Петро', 'Петрович', 'вул. Лесі Українки, 2', '0974567312', 2, 'Економіки', 'E-201', FALSE),
        (3, 'Сидоренко', 'Марія', 'Володимирівна', 'вул. Героїв Майдану, 3', '0506541270', 4, 'Аграрного менеджменту', 'A-401', FALSE),
        (4, 'Коваленко', 'Олександр', 'Олександрович', 'вул. Пушкінська, 4', '0736314210', 1, 'Інформаційні технології', 'IT-101', FALSE),
        (5, 'Мельник', 'Наталія', 'Іванівна', 'вул. Тарасівська, 5', '0964357912', 3, 'Аграрного менеджменту', 'A-301', FALSE),
        (6, 'Стеценко', 'Андрій', 'Вікторович', 'вул. Грушевського, 6', '0825341634', 2, 'Економіки', 'E-202', FALSE),
        (7, 'Козак', 'Оксана', 'Михайлівна', 'вул. Коцюбинського, 7', '0677431276', 4, 'Інформаційні технології', 'IT-401', TRUE),
        (8, 'Данилюк', 'Владислав', 'Ігорович', 'вул. Гоголя, 8', '0456421205', 1, 'Аграрного менеджменту', 'A-101', FALSE),
        (9, 'Савченко', 'Юлія', 'Андріївна', 'вул. Лисенка, 9', '0679431205', 3, 'Економіки', 'E-301', FALSE),
        (10, 'Павленко', 'Максим', 'Олександрович', 'вул. Петлюри, 10', '0551295410', 2, 'Інформаційні технології', 'IT-201', FALSE),
        (11, 'Кравченко', 'Анна', 'Сергіївна', 'вул. Червоноармійська, 11', '0889341206', 4, 'Аграрного менеджменту', 'A-401', FALSE),
        (12, 'Григоренко', 'Ігор', 'Олексійович', 'вул. Курчатова, 12', '0936542396', 1, 'Інформаційні технології', 'IT-102', FALSE),
        (13, 'Олійник', 'Валентина', 'Петрівна', 'вул. Гончара, 13', '0764360135', 3, 'Економіки', 'E-302', TRUE);
    """)
    query_for_db(connection, query)

    query = ("""
        INSERT INTO Subjects (ID, Name, Hours, Semesters)
        VALUES
        (1, 'Математика', 60, 2),
        (2, 'Програмування', 120, 4),
        (3, 'Фізика', 45, 2),
        (4, 'Графіка', 90, 3),
        (5, 'Бази даних', 80, 3),
        (6, 'Біологія', 30, 1),
        (7, 'Мови програмування', 100, 4),
        (8, 'Дизайн', 70, 2),
        (9, 'Англійська мова', 60, 2),
        (10, 'Економіка', 45, 1);
    """)
    query_for_db(connection, query)

    query = ('''
        INSERT INTO Exam (ID, Date, ID_student, ID_subject, Grade)
        VALUES
        (1, '2023-11-06', 4, 1, 4),
        (2, '2023-12-03', 7, 2, 5),
        (3, '2023-12-03', 2, 3, 3),
        (4, '2023-11-25', 1, 4, 5),
        (5, '2023-11-22', 10, 5, 4),
        (6, '2023-11-17', 5, 6, 3),
        (7, '2023-11-28', 7, 7, 5),
        (8, '2023-12-08', 8, 8, 4),
        (9, '2023-11-15', 9, 9, 3),
        (10, '2023-12-10', 2, 10, 5),
        (11, '2023-12-01', 3, 6, 4),
        (12, '2023-11-12', 13, 10, 3),
        (13, '2023-12-15', 6, 2, 5);
    ''')
    query_for_db(connection, query)

def query_for_db(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Запит виконано!")
    except Error as e:
        print(f"Помилка при виконанні запиту: {e}")

def query_with_print(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        df = pd.DataFrame(result, columns=[i[0] for i in cursor.description])
        print(df)
        print("Запит виконано!")
    except Error as e:
        print(f"Помилка при виконанні запиту: {e}")
    finally:
        cursor.close()

def queries_print(connection):
    connect = connection
    print("Відобразити всіх студентів, які є старостами, відсортувати прізвища за алфавітом")
    query = ("""
        SELECT * FROM Students
        WHERE Headman = TRUE
        ORDER BY Surname;
    """)
    query_with_print(connect, query)

    print("Порахувати середній бал для кожного студента (підсумковий запит)")
    query = ("""
        SELECT ID_student, AVG(Grade) AS Average_Grade
        FROM Exam
        GROUP BY ID_student;
    """)
    query_with_print(connect, query)

    print("Для кожного предмета порахувати загальну кількість годин, протягом яких він вивчається (запит з обчислювальним полем)")
    query = ("""
        SELECT Subjects.*, Hours * Semesters AS Total_Hours
        FROM Subjects;
    """)
    query_with_print(connect, query)

    print("Відобразити успішність студентів по обраному предмету (запит з параметром)")
    print("Введіть ID предмету:")
    ID_subject = input()
    query = (f"""
        SELECT Students.*, Exam.Grade
        FROM Students
        JOIN Exam ON Students.ID = Exam.ID_student
        WHERE Exam.ID_subject = {ID_subject};
    """)
    query_with_print(connect, query)

    print("Порахувати кількість студентів на кожному факультеті (підсумковий запит)")
    query = ("""
        SELECT Faculty AS 'Факультет', COUNT(*) AS 'Кількість студентів'
        FROM Students
        GROUP BY Faculty;
    """)
    query_with_print(connect, query)

    print("Відобразити оцінки кожного студента по кожному предмету (перехресний запит)")
    query = ("""
        SELECT Students.ID AS 'Номер студента', Surname AS 'Прізвище', Students.Name AS 'Імя', Patronymic AS 'По батькові', Subjects.ID AS 'Номер предмету', Subjects.Name AS 'Назва предменту', Grade AS 'Оцінка'
        FROM Students
        JOIN Exam ON Students.ID = Exam.ID_student
        JOIN Subjects ON Exam.ID_subject = Subjects.ID
        ORDER BY Students.ID, Subjects.ID;
    """)
    query_with_print(connect, query)

if __name__ == "__main__":
    config = {
        'server': '127.0.0.1',
        'login': 'root',
        'password': 'root',
        'database': 'University',
    }

    conn = connectionDB(**config)

    create_DB(conn)

    create_tables(conn)

    query_insert_for_tables(conn)

    conn = connectionDB(**config)
    queries_print(conn)
    conn.close()

    print("База даних створена та заповнена!")