import sqlite3
import re


# Совершение операции при просроченном или заблокированном паспорте.
def passport_fraud(conn, date):
	# DDMMYYYY -> YYYY-MM-DD
	date = re.sub(r'(\d\d)(\d\d)(\d{4})', r'\3-\2-\1', date)
	cursor = conn.cursor()
	
	cursor.execute('''
		CREATE TABLE if not exists STG_INVALID_CLIENTS AS
			SELECT
				client_id,
				last_name || ' ' || first_name || ' ' || patrinymic as fio,
				passport_num,
				phone
			FROM DWH_DIM_CLIENTS
			WHERE ? >  passport_valid_to
			OR passport_num in (SELECT passport_num FROM DWH_FACT_PASSPORT_BLACKLIST)
	''', [date])
	cursor.execute('''
		CREATE VIEW if not exists STG_V_PASSPORT_FRAUD AS
			SELECT
				t4.trans_date as event_dt,
				t1.passport_num as passport,
				t1.fio,
				t1.phone
			FROM STG_INVALID_CLIENTS t1
			INNER JOIN DWH_DIM_ACCOUNTS t2
			ON t1.client_id = t2.client
			INNER JOIN DWH_DIM_CARDS t3
			ON t2.account_num = t3.account_num
			INNER JOIN DWH_FACT_TRANSACTIONS t4
			ON t3.card_num = t4.card_num
	''')
	cursor.execute('''
		INSERT INTO REP_FRAUD (
			event_dt,
			passport,
			fio,
			phone,
			event_type
		) SELECT
			event_dt,
			passport,
			fio,
			phone,
			'passport_fraud'
		FROM STG_V_PASSPORT_FRAUD
		ORDER BY passport, event_dt
	''')
	cursor.execute('DROP TABLE if exists STG_INVALID_CLIENTS')
	cursor.execute('DROP VIEW if exists STG_V_PASSPORT_FRAUD')
	conn.commit()


# Совершение операции при недействующем договоре.
def account_fraud(conn,date):
	date = re.sub(r'(\d\d)(\d\d)(\d{4})', r'\3-\2-\1', date)
	cursor = conn.cursor()

	cursor.execute('''
		CREATE TABLE if not exists STG_INVALID_ACCOUNTS AS
			SELECT
				t1.account_num,
				t2.last_name || ' ' || t2.first_name || ' ' || t2.patrinymic as fio,
				t2.passport_num,
				t2.phone
			FROM DWH_DIM_ACCOUNTS t1
			LEFT JOIN DWH_DIM_CLIENTS t2
			ON t1.client = t2.client_id
			WHERE ? > t1.valid_to
	''', [date])
	cursor.execute('''
		CREATE VIEW if not exists STG_V_ACCOUNT_FRAUD AS
			SELECT
				t1.fio,
				t1.passport_num as passport,
				t1.phone,
				t3.trans_date as event_dt
			FROM STG_INVALID_ACCOUNTS t1
			INNER JOIN DWH_DIM_CARDS t2
			ON t1.account_num = t2.account_num
			INNER JOIN DWH_FACT_TRANSACTIONS t3
			ON t2.card_num = t3.card_num
	''')
	cursor.execute('''
		INSERT INTO REP_FRAUD (
			event_dt,
			passport,
			fio,
			phone,
			event_type
		) SELECT
			event_dt,
			passport,
			fio,
			phone,
			'account_fraud'
		FROM STG_V_ACCOUNT_FRAUD
		ORDER BY passport, event_dt
	''')
	cursor.execute('DROP TABLE if exists STG_INVALID_ACCOUNTS')
	cursor.execute('DROP VIEW if exists STG_V_ACCOUNT_FRAUD')
	conn.commit()


# Совершение операций в разных городах в течение одного часа.
def city_fraud(conn):
	cursor = conn.cursor()
	cursor.execute('''
		CREATE VIEW if not exists STG_V_CITY_TRANSACTIONS AS
			SELECT
				t1.card_num,
				t1.trans_date,
				t3.terminal_city
			FROM DWH_FACT_TRANSACTIONS t1
			INNER JOIN (
				SELECT
					t1.card_num,
					count(distinct t2.terminal_city) as cnt_city
				FROM DWH_FACT_TRANSACTIONS t1
				LEFT JOIN DWH_DIM_TERMINALS_HIST t2
				ON t1.terminal = t2.terminal_id
				GROUP BY t1.card_num
				HAVING count(distinct t2.terminal_city) > 1
			) t2
			ON t1.card_num = t2.card_num
			LEFT JOIN DWH_DIM_TERMINALS_HIST t3
			ON t1.terminal = t3.terminal_id	 	
	''')
	cursor.execute('''
		CREATE VIEW if not exists STG_V_LEAD_CITY_TRANSACTIONS AS
			SELECT
				card_num,
				max(trans_date) as trans_date
			FROM (
				SELECT
					card_num,
					trans_date,
					terminal_city,
					lead(trans_date) over(partition by card_num order by trans_date) as lead_trans_date,
					lead(terminal_city) over(partition by card_num order by trans_date) as lead_terminal_city
				FROM STG_V_CITY_TRANSACTIONS
			) t1
			WHERE terminal_city <> lead_terminal_city
			AND cast((julianday(trans_date) - julianday(lead_trans_date)) * 24 * 60 AS integer) <= 60
			GROUP BY card_num
	''')
	cursor.execute('''
		CREATE VIEW if not exists STG_V_CITY_FRAUD AS
			SELECT
				t1.trans_date as event_dt,
				t4.passport_num as passport,
				t4.last_name || ' ' || t4.first_name || ' ' || t4.patrinymic as fio,
				t4.phone
			FROM STG_V_LEAD_CITY_TRANSACTIONS t1
			LEFT JOIN DWH_DIM_CARDS t2
			ON t1.card_num = t2.card_num
			LEFT JOIN DWH_DIM_ACCOUNTS t3
			ON t2.account_num = t3.account_num
			LEFT JOIN DWH_DIM_CLIENTS t4
			ON t3.client = t4.client_id
	''')
	cursor.execute('''
		INSERT INTO REP_FRAUD (
			event_dt,
			passport,
			fio,
			phone,
			event_type
		) SELECT
			event_dt,
			passport,
			fio,
			phone,
			'city_fraud'
		FROM STG_V_CITY_FRAUD
	''')
	cursor.execute('DROP VIEW if exists STG_V_CITY_TRANSACTIONS')
	cursor.execute('DROP VIEW if exists STG_V_LEAD_CITY_TRANSACTIONS')
	cursor.execute('DROP VIEW if exists STG_V_CITY_FRAUD')
	conn.commit()

# Попытка подбора суммы.
def guessing_amount_fraud(conn):
	cursor = conn.cursor()
	cursor.execute('''
		CREATE VIEW if not exists STG_V_LAG_TRANSACTIONS AS
			SELECT
				card_num,
				trans_date,
				cast(amt as integer) as amt,
				lag(cast(amt as integer), 1) over(partition by card_num order by trans_date) as lag_amt_1,
				lag(cast(amt as integer), 2) over(partition by card_num order by trans_date) as lag_amt_2,
				lag(cast(amt as integer), 3) over(partition by card_num order by trans_date) as lag_amt_3,
				oper_result,
				lag(oper_result, 1) over(partition by card_num order by trans_date) as lag_result_1,
				lag(oper_result, 2) over(partition by card_num order by trans_date) as lag_result_2,
				lag(oper_result, 3) over(partition by card_num order by trans_date) as lag_result_3,
				lag(trans_date, 3) over(partition by card_num order by trans_date) as lag_trans_date
			FROM DWH_FACT_TRANSACTIONS
			WHERE oper_type in ('PAYMENT', 'WITHDRAW')
	''')
	cursor.execute('''
		CREATE VIEW if not exists STG_V_GUESSING_AMOUNT_FRAUD AS
			SELECT
				card_num,
				trans_date,
				amt,
				lag_amt_1,
				lag_amt_2,
				lag_amt_3,
				oper_result,
				lag_result_1,
				lag_result_2,
				lag_result_3,
				cast((julianday(trans_date) - julianday(lag_trans_date)) * 24 * 60 AS integer) as time_delta
			FROM STG_V_LAG_TRANSACTIONS
			WHERE oper_result = 'SUCCESS'
			AND lag_result_1 = 'REJECT'
			AND lag_result_2 = 'REJECT'
			AND lag_result_3 = 'REJECT'
			AND cast((julianday(trans_date) - julianday(lag_trans_date)) * 24 * 60 AS integer) <= 20
			AND lag_amt_3 > lag_amt_2 AND lag_amt_2 > lag_amt_1	AND lag_amt_1 > amt
	''')
	cursor.execute('''
		CREATE VIEW if not exists STG_V_GUESSING_AMOUNT_FRAUD_CLIENTS AS
			SELECT
				t1.trans_date as event_dt,
				t4.passport_num as passport,
				t4.last_name || ' ' || t4.first_name || ' ' || t4.patrinymic as fio,
				t4.phone
			FROM STG_V_GUESSING_AMOUNT_FRAUD t1
			LEFT JOIN DWH_DIM_CARDS t2
			ON t1.card_num = t2.card_num
			LEFT JOIN DWH_DIM_ACCOUNTS t3
			ON t2.account_num = t3.account_num
			LEFT JOIN DWH_DIM_CLIENTS t4
			ON t3.client = t4.client_id
	''')
	cursor.execute('''
		INSERT INTO REP_FRAUD (
			event_dt,
			passport,
			fio,
			phone,
			event_type
		) SELECT
			event_dt,
			passport,
			fio,
			phone,
			'guessing_amount_fraud'
		FROM STG_V_GUESSING_AMOUNT_FRAUD_CLIENTS
	''')
	cursor.execute('DROP VIEW if exists STG_V_LAG_TRANSACTIONS')
	cursor.execute('DROP VIEW if exists STG_V_GUESSING_AMOUNT_FRAUD')
	cursor.execute('DROP VIEW if exists STG_V_GUESSING_AMOUNT_FRAUD_CLIENTS')
	conn.commit()
