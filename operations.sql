-- Создание схем и ролей --
CREATE SCHEMA IF NOT EXISTS DS;
CREATE USER ds WITH PASSWORD 'dspass';
GRANT ALL ON SCHEMA DS TO ds;

CREATE SCHEMA IF NOT EXISTS LOGS;
CREATE USER logs WITH PASSWORD 'logspass';
GRANT ALL ON SCHEMA LOGS TO logs;


-- База данных для Airflow --
CREATE DATABASE airflow;
CREATE USER airflowuser WITH PASSWORD 'airflowpass';
GRANT ALL PRIVILEGES ON DATABASE airflow TO airflowuser;


-- Создание таблиц --
CREATE TABLE IF NOT EXISTS FT_BALANCE_F (
	on_date DATE NOT NULL,
	account_rk NUMERIC NOT NULL,
	currency_rk NUMERIC,
	balance_out FLOAT,
	UNIQUE (on_date, account_rk)
);

CREATE TABLE IF NOT EXISTS FT_POSTING_F (
	oper_date DATE NOT NULL,
	credit_account_rk NUMERIC NOT NULL,
	debet_account_rk NUMERIC NOT NULL,
	credit_amount FLOAT,
	debet_amount FLOAT
);

CREATE TABLE IF NOT EXISTS MD_ACCOUNT_D (
	data_actual_date DATE NOT NULL,
	data_actual_end_date DATE NOT NULL,
	account_rk NUMERIC NOT NULL,
	account_number VARCHAR(20) NOT NULL,
	char_type VARCHAR(1) NOT NULL,
	currency_rk NUMERIC NOT NULL,
	currency_code VARCHAR(3) NOT NULL,
	UNIQUE (data_actual_date, account_rk)
);

CREATE TABLE IF NOT EXISTS MD_CURRENCY_D (
	currency_rk NUMERIC NOT NULL,
	data_actual_date DATE NOT NULL,
	data_actual_end_date DATE,
	currency_code VARCHAR(3),
	code_iso_char VARCHAR(3),
	UNIQUE (currency_rk, data_actual_date)
);

CREATE TABLE IF NOT EXISTS MD_EXCHANGE_RATE_D (
	data_actual_date DATE NOT NULL,
	data_actual_end_date DATE,
	currency_rk NUMERIC NOT NULL,
	reduced_cource FLOAT,
	code_iso_num VARCHAR(3),
	UNIQUE (data_actual_date, currency_rk)
);

CREATE TABLE IF NOT EXISTS MD_LEDGER_ACCOUNT_S (
	chapter CHAR(1),
	chapter_name VARCHAR(16),
	section_number INTEGER,
	section_name VARCHAR(22),
	subsection_name VARCHAR(21),
	ledger1_account INTEGER,
	ledger1_account_name VARCHAR(47),
	ledger_account INTEGER not NULL,
	ledger_account_name VARCHAR(153),
	characteristic CHAR(1),
	is_resident INTEGER,
	is_reserve INTEGER,
	is_reserved INTEGER,
	is_loan INTEGER,
	is_reserved_assets INTEGER,
	is_overdue INTEGER,
	is_interest INTEGER,
	pair_account VARCHAR(5),
	start_date DATE not NULL,
	end_date DATE,
	is_rub_only INTEGER,
	min_term VARCHAR(1),
	min_term_measure VARCHAR(1),
	max_term VARCHAR(1),
	max_term_measure VARCHAR(1),
	ledger_acc_full_name_translit VARCHAR(1),
	is_revaluation VARCHAR(1),
	is_correct VARCHAR(1),
	UNIQUE (ledger_account, start_date)
);

-- Создание таблицы логгирования
CREATE TABLE IF NOT EXISTS logs.data_load_log (
    id INT PRIMARY KEY,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP NOT NULL,
    status VARCHAR(20) NOT NULL,
    error_message TEXT,
    file_name VARCHAR(255) NOT NULL,
    record_count INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
