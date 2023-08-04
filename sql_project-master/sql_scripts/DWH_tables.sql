DROP TABLE if exists DWH_DIM_CARDS;
CREATE TABLE if not exists DWH_DIM_CARDS(
	card_num varchar(128),
	account_num varchar(128),
	create_dt date,
	update_dt date
);


DROP TABLE if exists DWH_DIM_ACCOUNTS;
CREATE TABLE if not exists DWH_DIM_ACCOUNTS(
	account_num varchar(128),
	valid_to date,
	client varchar(128),
	create_dt date,
	update_dt date
);


DROP TABLE if exists DWH_DIM_CLIENTS;
CREATE TABLE if not exists DWH_DIM_CLIENTS(
	client_id varchar(128),
	last_name varchar(128),
	first_name varchar(128),
	patrinymic varchar(128),
	date_of_birth date,
	passport_num varchar(128),
	passport_valid_to date,
	phone varchar(128),
	create_dt date,
	update_dt date
);


DROP TABLE if exists DWH_FACT_PASSPORT_BLACKLIST;
CREATE TABLE if not exists DWH_FACT_PASSPORT_BLACKLIST(
	passport_num varchar(128),
	entry_dt date
);


DROP TABLE if exists DWH_FACT_TRANSACTIONS;
CREATE TABLE if not exists DWH_FACT_TRANSACTIONS(
	trans_id varchar(128),
	trans_date datetime,
	card_num varchar(128),
	oper_type varchar(128),
	amt decimal(10,2),
	oper_result varchar(128),
	terminal varchar(128)
);


CREATE TABLE if not exists REP_FRAUD(
	event_dt datetime,
	passport varchar(128),
	fio varchar(128),
	phone varchar(128),
	event_type varchar(128),
	report_dt date default current_timestamp
);