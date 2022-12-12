from faker import Faker
import random
import string
import psycopg2

files_number_main = 100
strings_number_main = 100_000
years_difference_main = 5
latin_string_len_main = 10
russian_string_len_main = 10
int_lower_bound_main = 1
int_upper_bound_main = 100_000_000
double_lower_bound_main = 1
double_upper_bound_main = 20
character_limit_main = 8

filter_string_main = 'abc'


# Метод генерирует необходимое количество файлов с заданными параметрами
def files_generation(files_number: int, strings_number: int,
                     years_difference: int,
                     latin_string_len: int, russian_string_len: int,
                     int_lower_bound: int, int_upper_bound: int,
                     double_lower_bound: int, double_upper_bound: int, character_limit: int):
    def cyrillic_string(length: int) -> str:
        answer = ''.join(chr(random.randint(ord('а'), ord('я'))) if random.normalvariate(0, 1) >= 0
                         else chr(random.randint(ord('А'), ord('Я'))) for _ in range(length))
        return answer

    fake = Faker()

    for i in range(files_number):
        w = open(f"file_{i}.txt", 'w', encoding='utf-8')

        for j in range(strings_number):
            random_date = fake.date_between(start_date=f'-{years_difference}y',
                                            end_date='today').strftime('%d.%m.%Y')
            random_latin_string = ''.join(random.choice(string.ascii_letters) for i in range(latin_string_len))
            random_cyrillic_string = cyrillic_string(russian_string_len)
            if int_lower_bound % 2 == 1:
                int_lower_bound -= 1
            random_int_number = random.randrange(int_lower_bound, int_upper_bound, 2)
            random_double_number = round(
                random.random() * (double_upper_bound - double_lower_bound) + double_lower_bound, character_limit)

            w.write(
                f'{random_date}||{random_latin_string}||{random_cyrillic_string}||{random_int_number}||{random_double_number}\n')

        print(f'file_{i} is ready!')


# Метод объединяет файлы в один с предшествующей объединению фильтрации по строке
def merge_files_and_filter(filter_string: str):
    deleted_lines = 0
    with open('merged_files.txt', 'w', encoding='utf-8') as outfile:
        for i in range(files_number_main):
            with open(f'file_{i}.txt', 'r', encoding='utf-8', errors='ignore') as infile:
                lines = infile.readlines()
            with open(f'file_{i}.txt', 'w', encoding='utf-8') as file:
                for line in lines:
                    if line.find(filter_string) == -1:
                        file.write(line)
                    else:
                        deleted_lines += 1
            with open(f'file_{i}.txt', 'r', encoding='utf-8', errors='ignore') as infile:
                outfile.write(infile.read().replace('||', '|'))
    print(f'Total number deleted lines: {deleted_lines}')

    with open('merged_files.txt', 'r', encoding='utf-8') as outfile:
        lines_num = len(outfile.readlines())
        print(f'Total number of merged file lines before filter usage: {lines_num}')


# База данных создана отдельным скриптом, этот метод заполняет таблицу
# сгенерированными выше данными
def database_table_data():
    conn = psycopg2.connect(user='postgres',
                            password='password',
                            host='localhost',
                            port='5432')

    cur = conn.cursor()

    with open('merged_files.txt', 'r', encoding='utf-8') as file:
        file_content = file.readlines()
        values = [line.strip().split('|') for line in file_content]

    for i in range(len(values)):
        cur.execute('''INSERT INTO random_data VALUES (%s, %s, %s, %s, %s);''', values[i])
        conn.commit()
        print(f'Has been imported: {i + 1} lines, will be imported: {len(values) - i - 1} lines')

    cur.execute("SELECT * FROM random_data ")
    for line in cur.fetchall():
        print(line)

    cur.close()
    conn.commit()


# Метод подсчитывает сумму целых и медиану вещественных чисел через sql-запросы
def count_sum_and_median():
    conn = psycopg2.connect(user='postgres',
                            password='password',
                            host='localhost',
                            port='5432')

    cur = conn.cursor()

    cur.execute("SELECT SUM(random_integer_number) FROM random_data;")
    print(f'Sum of integer numbers: {cur.fetchall()}')
    conn.commit()

    cur.execute("SELECT PERCENTILE_CONT(0.5) "
                "WITHIN GROUP(ORDER BY random_double_number) FROM random_data;")
    print(f'Median of double numbers: {cur.fetchall()}')
    conn.commit()


if __name__ == '__main__':
    files_generation(files_number_main, strings_number_main,
                     years_difference_main,
                     latin_string_len_main, russian_string_len_main,
                     int_lower_bound_main, int_upper_bound_main,
                     double_lower_bound_main, double_upper_bound_main, character_limit_main)

    merge_files_and_filter(filter_string_main)

    database_table_data()

    count_sum_and_median()
