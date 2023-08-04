import sqlite3
import pandas as pd
import os


# функция загрузки данных из sql файла
def sql_to_db(conn, path):
	cursor = conn.cursor()
	with open(path, 'r', encoding='utf-8') as sql_file:
		sql_script = sql_file.read()
		cursor.executescript(sql_script)
		conn.commit()


# функция загрузки данных из txt файла
def csv_to_sql(conn, path, table_name):
	df = pd.read_csv(path, sep=';')
	df.to_sql(table_name, con=conn, if_exists='replace', index=False)


# функция загрузки данных из xlsx файла
def xlsx_to_sql(conn, path, table_name):
	df = pd.read_excel(path)
	df.to_sql(table_name, con=conn, if_exists='replace', index=False)


# функция загрузки данных из ddl_dml.sql в DWH_tables
def data_to_DWH(conn, path):
	sql_to_db(conn, path)
	cursor = conn.cursor()
	cursor.execute('''
		INSERT INTO DWH_DIM_CARDS (
			card_num,
			account_num,
			create_dt,
			update_dt
		) SELECT
			card_num,
			account,
			create_dt,
			update_dt
		FROM cards
	''')
	cursor.execute('''
		INSERT INTO DWH_DIM_ACCOUNTS (
			account_num,
			valid_to,
			client,
			create_dt,
			update_dt
		) SELECT
			account,
			valid_to,
			client,
			create_dt,
			update_dt
		FROM accounts
	''')
	cursor.execute('''
		INSERT INTO DWH_DIM_CLIENTS (
			client_id,
			last_name,
			first_name,
			patrinymic,
			date_of_birth,
			passport_num,
			passport_valid_to,
			phone,
			create_dt,
			update_dt
		) SELECT
			client_id, 
		    last_name, 
		    first_name, 
		    patronymic, 
		    date_of_birth, 
		    passport_num, 
		    passport_valid_to, 
		    phone,
		    create_dt, 
		    update_dt
		FROM clients
	''')
	cursor.execute('DROP TABLE if exists cards')
	cursor.execute('DROP TABLE if exists accounts')
	cursor.execute('DROP TABLE if exists clients')
	conn.commit()


# функция загрузки транзакций в базу данных
def transactions_to_DWH(conn, date):
	source = 'data/transactions_' + date + '.txt'
	csv_to_sql(conn, source, 'STG_TRANSACTIONS')
	cursor = conn.cursor()

	cursor.execute('''
		INSERT INTO DWH_FACT_TRANSACTIONS (
			trans_id,
			trans_date,
			amt,
			card_num,
			oper_type,
			oper_result,
			terminal
		) SELECT
			transaction_id,
			transaction_date,
			amount,
			card_num,
			oper_type,
			oper_result,
			terminal
		FROM STG_TRANSACTIONS
	''')
	cursor.execute('DROP TABLE if exists STG_TRANSACTIONS')
	conn.commit()

	backup_source = os.path.join('archive', 'transactions_' + date + '.txt.backup')
	os.rename(source, backup_source)


# функция загрузки паспортов в черном списке в базу данных
def passport_blacklist_to_DWH(conn, date):
	source = 'data/passport_blacklist_' + date + '.xlsx'
	xlsx_to_sql(conn, source, 'STG_PASSPORT_BLACKLIST')
	cursor = conn.cursor()

	cursor.execute('''
		INSERT INTO DWH_FACT_PASSPORT_BLACKLIST (
			entry_dt,
			passport_num
		) SELECT
			date,
			passport
		FROM STG_PASSPORT_BLACKLIST
	''')
	cursor.execute('DROP TABLE if exists STG_PASSPORT_BLACKLIST')
	conn.commit()

	backup_source = os.path.join('archive', 'passport_blacklist_' + date + '.xlsx.backup')
	os.rename(source, backup_source)


# функция создания исторической таблицы и представления с терминалами 
def init_terminals_hist(cursor):
	cursor.execute('''
		CREATE TABLE if not exists DWH_DIM_TERMINALS_HIST(
			terminal_id varchar(128),
			terminal_type varchar(128),
			terminal_city varchar(128),
			terminal_address varchar(128),
			effective_from datetime default current_timestamp,
			effective_to datetime default (datetime('2999-12-31 23:59:59')),
			deleted_flg integer default 0
		)
	''')

	cursor.execute('''
		CREATE VIEW if not exists STG_V_TERMINALS AS 
			SELECT
				terminal_id,
				terminal_type,
				terminal_city,
				terminal_address
			FROM DWH_DIM_TERMINALS_HIST
			WHERE deleted_flg = 0
			AND current_timestamp between effective_from and effective_to
	''')

# таблица с новыми данными из терминалов
def create_new_rows(cursor):
	cursor.execute('''
		CREATE TABLE STG_NEW_TERMINALS AS 
			SELECT
				t1.*
			FROM STG_TERMINALS t1
			LEFT JOIN STG_V_TERMINALS t2
			ON t1.terminal_id = t2.terminal_id
			WHERE t2.terminal_id is null
	''')

# таблица с удаленными данными из терминалов
def create_deleted_rows(cursor):
	cursor.execute('''
		CREATE TABLE STG_DELETED_TERMINALS AS
			SELECT
				t1.*
			FROM STG_V_TERMINALS t1
			LEFT JOIN STG_TERMINALS t2
			ON t1.terminal_id = t2.terminal_id
			WHERE t2.terminal_id is null
	''')

# таблица с измененными данными из терминалов
def create_changed_rows(cursor):
	cursor.execute('''
		CREATE TABLE STG_CHANGED_TERMINALS AS
			SELECT
				t1.*
			FROM STG_TERMINALS t1
			INNER JOIN STG_V_TERMINALS t2
			ON t1.terminal_id = t2.terminal_id
			AND (t1.terminal_type <> t2.terminal_type
			OR t1.terminal_city <> t2.terminal_city
			OR t1.terminal_address <> t2.terminal_address)
	''')

# функция изменения исторической таблицы терминалов
def update_terminals_hist(conn):
	cursor = conn.cursor()
	# добавляем новые данные
	cursor.execute('''
		INSERT INTO DWH_DIM_TERMINALS_HIST (
			terminal_id,
			terminal_type,
			terminal_city,
			terminal_address
		) SELECT 
			terminal_id,
			terminal_type,
			terminal_city,
			terminal_address
		FROM STG_NEW_TERMINALS
	''')

	# актуализируем удаленные данные в исторической таблице
	cursor.execute('''
		UPDATE DWH_DIM_TERMINALS_HIST
		SET effective_to = datetime('now', '-1 second')
		WHERE terminal_id in (SELECT terminal_id FROM STG_DELETED_TERMINALS)
		AND effective_to = datetime('2999-12-31 23:59:59')
	''')
	# добавляем удаленные данные
	cursor.execute('''
		INSERT INTO DWH_DIM_TERMINALS_HIST (
			terminal_id,
			terminal_type,
			terminal_city,
			terminal_address,
			deleted_flg
		) SELECT
			terminal_id,
			terminal_type,
			terminal_city,
			terminal_address,
			1
		FROM STG_DELETED_TERMINALS
	''')

	# актуализируем измененные данные в исторической таблице
	cursor.execute('''
		UPDATE DWH_DIM_TERMINALS_HIST
		SET effective_to = datetime('now', '-1 second')
		WHERE terminal_id in (SELECT terminal_id FROM STG_CHANGED_TERMINALS)
		AND effective_to = datetime('2999-12-31 23:59:59')
	''')
	# добавляем измененные данные
	cursor.execute('''
		INSERT INTO DWH_DIM_TERMINALS_HIST (
			terminal_id,
			terminal_type,
			terminal_city,
			terminal_address
		) SELECT
			terminal_id,
			terminal_type,
			terminal_city,
			terminal_address
		FROM STG_CHANGED_TERMINALS
	''')
	conn.commit()

# функция удаления временных таблиц терминалов
def drop_stg_terminals(conn):
	cursor = conn.cursor()
	cursor.execute('DROP TABLE if exists STG_TERMINALS')
	cursor.execute('DROP TABLE if exists STG_NEW_TERMINALS')
	cursor.execute('DROP TABLE if exists STG_DELETED_TERMINALS')
	cursor.execute('DROP TABLE if exists STG_CHANGED_TERMINALS')

# функция загрузки терминалов в базу данных (SCD2)
def terminals_to_DWH(conn, date):
	source = 'data/terminals_' + date + '.xlsx'
	xlsx_to_sql(conn, source, 'STG_TERMINALS')
	cursor = conn.cursor()

	init_terminals_hist(cursor)
	create_new_rows(cursor)
	create_deleted_rows(cursor)
	create_changed_rows(cursor)
	update_terminals_hist(conn)
	drop_stg_terminals(conn)

	backup_source = os.path.join('archive', 'terminals_' + date + '.xlsx.backup')
	os.rename(source, backup_source)