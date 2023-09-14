import psycopg2
import random
from psycopg2 import OperationalError, Error

# try:
#     # Подключение к базе данных PostgreSQL в контейнере Docker
connection = psycopg2.connect(
    host="localhost",
    port="5432",
    user="user",
    password="123",
    database="db-my"
)

# Если нет исключения, значит, подключение успешно
print("Подключение к базе данных PostgreSQL успешно установлено.")


# except psycopg2.Error as e:
#     # Если возникло исключение, выводим сообщение об ошибке
#     print("Ошибка при подключении к базе данных PostgreSQL:", e)


def create_table_users():
    try:
        with connection.cursor() as cursor:
            create_table_query = '''
                CREATE TABLE users (
                    user_id INT PRIMARY KEY,
                    first_name VARCHAR(50),
                    last_name VARCHAR(50),
                    age INT,
                    gender VARCHAR(10),
                    email VARCHAR(100),
                    country VARCHAR(50)
                );
            '''

            cursor.execute(create_table_query)
            connection.commit()
            print('Таблица успешно создана!')

    except (Exception, Error) as error:
        print("Ошибка при создании таблицы:", error)

def create_table_orders():
    try:
        with connection.cursor() as cursor:
            create_table_query = '''
                CREATE TABLE orders (
                    order_id SERIAL PRIMARY KEY,
                    user_id INT,
                    order_date DATE,
                    total_amount DECIMAL(10, 2),
                    FOREIGN KEY (user_id) REFERENCES users (user_id)
                );
            '''

            cursor.execute(create_table_query)
            connection.commit()
            print('Таблица успешно создана!')

    except (Exception, Error) as error:
        print("Ошибка при создании таблицы:", error)

def generate_random_data_users(start_id, num_rows):
    first_names = [
        'Jack', 'Emily', 'Ethan', 'Ava', 'Oliver', 'Isabella', 'Harry', 'Sophia', 'George', 'Mia',
        'Noah', 'Amelia', 'Charlie', 'Harper', 'William', 'Evelyn', 'Thomas', 'Abigail', 'James', 'Elizabeth',
        'Joshua', 'Sofia', 'Henry', 'Lily', 'Samuel', 'Grace', 'Alexander', 'Chloe', 'Daniel', 'Victoria',
        'Oscar', 'Scarlett', 'Joseph', 'Aria', 'Muhammad', 'Zoey', 'Archie', 'Lillian', 'Leo', 'Layla',
        'David', 'Natalie', 'Benjamin', 'Camila', 'Freddie', 'Hannah', 'Lewis', 'Zoe', 'Toby', 'Nora'
    ]
    last_names = [
        'Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Miller', 'Davis', 'Garcia', 'Rodriguez', 'Martinez',
        'Hernandez', 'Lopez', 'Gonzalez', 'Wilson', 'Anderson', 'Thomas', 'Taylor', 'Moore', 'Jackson', 'Martin',
        'Lee', 'Perez', 'Thompson', 'White', 'Harris', 'Sanchez', 'Clark', 'Ramirez', 'Lewis', 'Robinson',
        'Walker', 'Young', 'Allen', 'King', 'Wright', 'Scott', 'Torres', 'Nguyen', 'Hill', 'Flores',
        'Green', 'Adams', 'Nelson', 'Baker', 'Hall', 'Rivera', 'Campbell', 'Mitchell', 'Carter', 'Roberts'
    ]
    genders = ["Male", "Female"]
    countries = [
        'New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Philadelphia', 'San Antonio', 'San Diego', 'Dallas', 'San Jose',
        'Austin', 'Jacksonville', 'Fort Worth', 'Columbus', 'Charlotte', 'San Francisco', 'Indianapolis', 'Seattle', 'Denver', 'Washington',
        'Boston', 'El Paso', 'Nashville', 'Detroit', 'Oklahoma City', 'Portland', 'Las Vegas', 'Memphis', 'Louisville', 'Baltimore',
        'Milwaukee', 'Albuquerque', 'Tucson', 'Fresno', 'Sacramento', 'Mesa', 'Kansas City', 'Atlanta', 'Long Beach', 'Colorado Springs',
        'Raleigh', 'Miami', 'Virginia Beach', 'Omaha', 'Oakland', 'Minneapolis', 'Tulsa', 'Arlington', 'New Orleans', 'Wichita'
    ]

    data = []
    for i in range(start_id + 1, start_id + num_rows + 1):
        first_name = random.choice(first_names)
        last_name = random.choice(last_names)
        age = random.randint(18, 60)
        gender = random.choice(genders)
        email = f"{first_name.lower()}.{last_name.lower()}@example.com"
        country = random.choice(countries)
        data.append((i, first_name, last_name, age, gender, email, country))

    print(f"Данные успешно сгенерированы: {num_rows} строк.")

    return data

def insert_data_to_table_users(data, table_name, connection):
    try:
        with connection.cursor() as cursor:
            query = f'''
                INSERT INTO {table_name} (user_id, first_name, last_name, age, gender, email, country)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
--                     ON CONFLICT (user_id) DO NOTHING;
            '''

            cursor.executemany(query, data)
            connection.commit()
            cursor.close()
            print(f"Данные успешно добавлены в таблицу {table_name}.")

    except (Exception, Error) as error:
        print("Ошибка при добавлении данных в таблицу:", error)


def get_max_user_id(connection, table_name):
    try:
        with connection.cursor() as cursor:
            query = f'''
                SELECT COALESCE(MAX(user_id), 0)
                    FROM {table_name};
            '''
            cursor.execute(query)
            max_user_id = cursor.fetchone()[0]
            cursor.close()

            return max_user_id

    except (Exception, Error) as error:
        print(f"Ошибка при поиске максимального id в таблице {table_name}:", error)


def generate_random_data_orders(num_rows):

    data = []
    for i in range(1, num_rows + 1):
        user_id = random.randint(1, 20_000_000)
        order_date = f"2020-01-{random.randint(1, 31):02d}"
        total_amount = round(random.uniform(10, 1000), 2)

        data.append((i, user_id, order_date, total_amount))

    print(f"Данные успешно сгенерированы: {num_rows} строк.")

    return data


def insert_data_to_table_orders(data, table_name, connection):
    try:
        with connection.cursor() as cursor:
            query = f'''
                INSERT INTO {table_name} (order_id, user_id, order_date, total_amount)
                    VALUES (%s, %s, %s, %s)
            '''

            cursor.executemany(query, data)
            connection.commit()
            cursor.close()
            print(f"Данные успешно добавлены в таблицу {table_name}.")

    except (Exception, Error) as error:
        print("Ошибка при добавлении данных в таблицу:", error)

if __name__ == "__main__":
    # create_table_users()
    # create_table_orders()

    # table_name = 'users'
    # num_rows_to_insert = 5
    # max_user_id = get_max_user_id(connection, table_name)
    # data_to_insert = generate_random_data_users(max_user_id, num_rows_to_insert)
    # insert_data_to_table_users(data_to_insert, table_name, connection)

    table_name = 'orders'
    num_rows_to_insert = 10_000_000
    data_to_insert = generate_random_data_orders(num_rows_to_insert)
    insert_data_to_table_orders(data_to_insert, table_name, connection)
