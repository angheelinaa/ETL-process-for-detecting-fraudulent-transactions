import sqlite3
import os


def show_data(conn, source):
    cursor = conn.cursor()
    print('_-' * 20)
    print(source)
    print('_-' * 20)
    cursor.execute(f'SELECT * FROM {source}')
    for row in cursor.fetchall():
        print(row)
    print('_-' * 20)


def get_date_from_file(directory):
    date = ''
    list_of_files = os.listdir(directory)
    list_of_files.sort()
    
    for filename in list_of_files:
        if filename.endswith('.txt'):
            date = filename.split('_')[1].split('.')[0]
            break

    if date == '':
        raise Exception('Файлы не найдены')

    return date
