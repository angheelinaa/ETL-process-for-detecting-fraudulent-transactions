import sqlite3
from py_scripts import data_to_db, tools, rep_fraud

conn = sqlite3.connect('database.db')

data_to_db.sql_to_db(conn, 'sql_scripts/DWH_tables.sql')  # создали все таблицы в хранилище
data_to_db.data_to_DWH(conn, 'ddl_dml.sql')  # добавили данные из sql файла в хранилище

try:
	date = tools.get_date_from_file('data')  # извлекли дату из названия файла
except Exception as e:
	print(e)
	quit()

data_to_db.transactions_to_DWH(conn, date)  # загрузили транзакции
data_to_db.passport_blacklist_to_DWH(conn, date)  # загрузили заблокированные паспорта
data_to_db.terminals_to_DWH(conn, date)  # загрузили терминалы

# поиск мошеннических операций
rep_fraud.passport_fraud(conn, date)
rep_fraud.account_fraud(conn, date)
rep_fraud.city_fraud(conn)
rep_fraud.guessing_amount_fraud(conn)

# вывод витрины отчетности
tools.show_data(conn, 'REP_FRAUD')





