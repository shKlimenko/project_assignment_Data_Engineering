-- Проектное задание 1.2

-- процедура заполнения витрины 'Обороты по лицевым счетам' за один день (с логгированием)
CREATE OR REPLACE PROCEDURE ds.fill_account_turnover_f(i_OnDate DATE)
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc_name TEXT := 'ds.fill_account_turnover_f';
    v_start_time TIMESTAMP;
    v_end_time   TIMESTAMP;
    v_rows_affected INT := 0;
BEGIN
    v_start_time := clock_timestamp();
   
    DELETE FROM DM.DM_ACCOUNT_TURNOVER_F WHERE on_date = i_OnDate;

    WITH 
        credit_turnover AS (
            SELECT 
                credit_account_rk AS account_rk,
                SUM(credit_amount) AS credit_amount
            FROM ds.FT_POSTING_F
            WHERE oper_date = i_OnDate
            GROUP BY credit_account_rk
        ),
        debet_turnover AS (
            SELECT 
                debet_account_rk AS account_rk,
                SUM(debet_amount) AS debet_amount
            FROM ds.FT_POSTING_F
            WHERE oper_date = i_OnDate
            GROUP BY debet_account_rk
        ),
        currency_rates AS (
            SELECT 
                currency_rk,
                reduced_cource AS rate
            FROM ds.MD_EXCHANGE_RATE_D
            WHERE data_actual_date <= i_OnDate 
              AND (data_actual_end_date IS NULL OR data_actual_end_date > i_OnDate)
        )
    INSERT INTO DM.DM_ACCOUNT_TURNOVER_F (
        on_date,
        account_rk,
        credit_amount,
        credit_amount_rub,
        debet_amount,
        debet_amount_rub
    )
    SELECT 
        i_OnDate AS on_date,
        acc.account_rk,
        COALESCE(ct.credit_amount, 0),
        COALESCE(ct.credit_amount, 0) * COALESCE(cr.rate, 1),
        COALESCE(dt.debet_amount, 0),
        COALESCE(dt.debet_amount, 0) * COALESCE(cr.rate, 1)
    FROM 
        (SELECT DISTINCT account_rk FROM ds.MD_ACCOUNT_D) acc
    LEFT JOIN credit_turnover ct ON acc.account_rk = ct.account_rk
    LEFT JOIN debet_turnover dt ON acc.account_rk = dt.account_rk
    LEFT JOIN ds.MD_ACCOUNT_D a ON acc.account_rk = a.account_rk 
        AND a.data_actual_date <= i_OnDate 
        AND (a.data_actual_end_date IS NULL OR a.data_actual_end_date > i_OnDate)
    LEFT JOIN currency_rates cr ON a.currency_rk = cr.currency_rk
    WHERE ct.credit_amount IS NOT NULL AND dt.debet_amount IS NOT NULL;

    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;

    v_end_time := clock_timestamp();

    IF v_rows_affected > 0 THEN
        INSERT INTO logs.proc_log (proc_name, on_date, start_time, end_time, row_count)
    VALUES (v_proc_name, i_OnDate, v_start_time, v_end_time, v_rows_affected);
    ELSE
        RAISE NOTICE 'Нет данных для даты %', i_OnDate;
    END IF;

END;
$$;


-- процедура заполнения витрины 'Обороты по лицевым счетам' за выбранный период
CREATE OR REPLACE PROCEDURE ds.fill_account_turnover_period(start_date DATE, end_date DATE)
LANGUAGE plpgsql
AS $$
DECLARE
    d DATE;
BEGIN
    FOR d IN 
        SELECT generate_series(start_date, end_date, interval '1 day')::date
    LOOP
        RAISE NOTICE 'Обработка даты: %', d;
        CALL ds.fill_account_turnover_f(d);
    END LOOP;
END;
$$;



-----------------------------------------------------------------------------



-- процедура заполнения витрины 'Остатки по лицевым счетам' за 2017-12-31 (с логгированием) 
CREATE OR REPLACE PROCEDURE ds.fill_account_balance_2017_12_31_f()
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc_name TEXT := 'ds.fill_account_balance_2017_12_31_f';
    v_start_time TIMESTAMP;
    v_end_time   TIMESTAMP;
    v_rows_affected INT := 0;
BEGIN
    v_start_time := clock_timestamp();

    DELETE FROM DM.DM_ACCOUNT_BALANCE_F WHERE on_date = '2017-12-31';

    INSERT INTO DM.DM_ACCOUNT_BALANCE_F (
        on_date,
        account_rk,
    	balance_out,
        balance_out_rub
    )
    SELECT 
        b.on_date,
        b.account_rk,
	    b.balance_out,
        b.balance_out * COALESCE(er.reduced_cource, 1) AS balance_out_rub
    FROM ds.FT_BALANCE_F b
    LEFT JOIN (
        SELECT currency_rk, reduced_cource
        FROM ds.MD_EXCHANGE_RATE_D
        WHERE data_actual_date <= '2017-12-31'
          AND (data_actual_end_date IS NULL OR data_actual_end_date > '2017-12-31')
    ) er ON b.currency_rk = er.currency_rk
    WHERE b.on_date = '2017-12-31'
	;

    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;

    v_end_time := clock_timestamp();

    IF v_rows_affected > 0 THEN
        INSERT INTO logs.proc_log (proc_name, on_date, start_time, end_time, row_count)
        VALUES (v_proc_name, '2017-12-31', v_start_time, v_end_time, v_rows_affected);
    ELSE
        RAISE NOTICE 'Нет данных для даты 2017-12-31';
    END IF;

END;
$$;



-- процедура заполнения витрины 'Остатки по лицевым счетам' за один день (с логгированием)
CREATE OR REPLACE PROCEDURE ds.fill_account_balance_f(i_OnDate DATE)
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc_name TEXT := 'ds.fill_account_balance_f';
    v_start_time TIMESTAMP;
    v_end_time   TIMESTAMP;
    v_rows_affected INT := 0;
BEGIN
    v_start_time := clock_timestamp();

    DELETE FROM DM.DM_ACCOUNT_BALANCE_F WHERE on_date = i_OnDate;

    INSERT INTO DM.DM_ACCOUNT_BALANCE_F (
        on_date,
        account_rk,
		balance_out,
        balance_out_rub
    )
    SELECT 
        i_OnDate AS on_date,
        a.account_rk,
        CASE 
            WHEN a.char_type = 'А' THEN COALESCE(ldb.balance_out, 0) + COALESCE(tdb.debet_amount, 0) - COALESCE(tdb.credit_amount, 0)
            WHEN a.char_type = 'П' THEN COALESCE(ldb.balance_out, 0) - COALESCE(tdb.debet_amount, 0) + COALESCE(tdb.credit_amount, 0)
        END AS balance_out,
        CASE 
            WHEN a.char_type = 'А' THEN COALESCE(ldb.balance_out_rub, 0) + COALESCE(tdb.debet_amount_rub, 0) - COALESCE(tdb.credit_amount_rub, 0)
            WHEN a.char_type = 'П' THEN COALESCE(ldb.balance_out_rub, 0) - COALESCE(tdb.debet_amount_rub, 0) + COALESCE(tdb.credit_amount_rub, 0)
        END AS balance_out_rub
    FROM (
        SELECT DISTINCT account_rk, char_type
        FROM ds.MD_ACCOUNT_D
        WHERE data_actual_date <= i_OnDate
          AND (data_actual_end_date IS NULL OR data_actual_end_date >= i_OnDate)
          AND char_type IN ('А', 'П')
    ) a
    LEFT JOIN (
        SELECT account_rk, balance_out, balance_out_rub
        FROM DM.DM_ACCOUNT_BALANCE_F
        WHERE on_date = i_OnDate - INTERVAL '1 day'
    ) ldb ON a.account_rk = ldb.account_rk
    LEFT JOIN (
        SELECT account_rk, credit_amount, debet_amount, credit_amount_rub, debet_amount_rub
        FROM DM.DM_ACCOUNT_TURNOVER_F
        WHERE on_date = i_OnDate
    ) tdb ON a.account_rk = tdb.account_rk;

    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;

    v_end_time := clock_timestamp();

    IF v_rows_affected > 0 THEN
        INSERT INTO logs.proc_log (proc_name, on_date, start_time, end_time, row_count)
        VALUES (v_proc_name, i_OnDate, v_start_time, v_end_time, v_rows_affected);
    ELSE
        RAISE NOTICE 'Нет данных для даты %', i_OnDate;
    END IF;

END;
$$;


-- процедура заполнения витрины 'Остатки по лицевым счетам' за выбранный период
CREATE OR REPLACE PROCEDURE ds.fill_account_balance_period(start_date DATE, end_date DATE)
LANGUAGE plpgsql
AS $$
DECLARE
    d DATE;
BEGIN
    FOR d IN 
        SELECT generate_series(start_date, end_date, interval '1 day')::date
    LOOP
        RAISE NOTICE 'Обработка даты: %', d;
        CALL ds.fill_account_balance_f(d);
    END LOOP;
END;
$$;






-- блок для проверки
-- очищаем все витрины и логи
TRUNCATE TABLE DM.DM_ACCOUNT_TURNOVER_F;
TRUNCATE TABLE DM.DM_ACCOUNT_BALANCE_F;
TRUNCATE TABLE LOGS.proc_log;

-- заполняем витрину оборотов
CALL ds.fill_account_turnover_period('2018-01-01', '2018-01-31');

-- проверяем витрину оборотов и логи
SELECT * FROM DM.DM_ACCOUNT_TURNOVER_F;
SELECT * FROM LOGS.proc_log;

-- заполняем витрину остатков за 2017-12-31
CALL ds.fill_account_balance_2017_12_31_f();

-- проверяем витрину остатков и логи
SELECT * FROM DM.DM_ACCOUNT_BALANCE_F;
SELECT * FROM LOGS.proc_log;

-- заполняем витрину остатков за январь 2018
CALL ds.fill_account_balance_period('2018-01-01', '2018-01-31');

-- проверяем витрину остатков и логи
SELECT * FROM DM.DM_ACCOUNT_BALANCE_F;
SELECT * FROM LOGS.proc_log;




-- Проектное задание 1.3
-- процедура заполнения витрины с данными по 101 форме (с логгированием)
CREATE OR REPLACE PROCEDURE dm.fill_f101_round_f(i_OnDate DATE)
LANGUAGE plpgsql
AS $$
DECLARE
    v_FromDate DATE;
    v_ToDate DATE;
    v_PrevDay DATE;
	v_proc_name TEXT := 'dm.fill_f101_round_f';
    v_start_time TIMESTAMP;
    v_end_time   TIMESTAMP;
    v_rows_affected INT := 0;
BEGIN
    v_start_time := clock_timestamp();

    v_FromDate := DATE_TRUNC('month', i_OnDate) - INTERVAL '1 month';
    v_ToDate := DATE_TRUNC('month', i_OnDate) - INTERVAL '1 day';
    v_PrevDay := v_FromDate - INTERVAL '1 day'; 
    
    DELETE FROM DM.DM_F101_ROUND_F WHERE FROM_DATE = v_FromDate AND TO_DATE = v_ToDate;
    
    INSERT INTO DM.DM_F101_ROUND_F (
        FROM_DATE, TO_DATE, CHAPTER, LEDGER_ACCOUNT, CHARACTERISTIC,
        BALANCE_IN_RUB, BALANCE_IN_VAL, BALANCE_IN_TOTAL,
        TURN_DEB_RUB, TURN_DEB_VAL, TURN_DEB_TOTAL,
        TURN_CRE_RUB, TURN_CRE_VAL, TURN_CRE_TOTAL,
        BALANCE_OUT_RUB, BALANCE_OUT_VAL, BALANCE_OUT_TOTAL
    )
    WITH 
	accounts AS (
        SELECT 
			a.account_rk,
			a.account_number,
		    SUBSTRING(a.account_number, 1, 5) AS ledger_account,
		    a.char_type AS characteristic,
		    l.characteristic characteristic2,
		    l.chapter AS chapter,
		    a.currency_code AS currency_code
		FROM DS.MD_ACCOUNT_D a
		JOIN DS.MD_LEDGER_ACCOUNT_S l ON SUBSTRING(a.account_number, 1, 5) = l.ledger_account::TEXT
		WHERE a.data_actual_date <= v_ToDate 
		  AND (a.data_actual_end_date IS NULL OR a.data_actual_end_date >= v_FromDate)
    ),
	start_balances AS (
        SELECT 
            a.ledger_account,
            a.characteristic,
            a.chapter,
            SUM(CASE WHEN a.currency_code IN ('643', '810') THEN b.balance_out_rub ELSE 0 END) AS balance_in_rub,
            SUM(CASE WHEN a.currency_code NOT IN ('643', '810') THEN b.balance_out_rub ELSE 0 END) AS balance_in_val,
            SUM(b.balance_out_rub) AS balance_in_total
        FROM accounts a
        JOIN DM.DM_ACCOUNT_BALANCE_F b ON a.account_rk = b.account_rk
		WHERE b.on_date = v_PrevDay
		GROUP BY a.ledger_account, a.characteristic, a.chapter
    ),
	end_balances AS (
        SELECT 
            a.ledger_account,
            SUM(CASE WHEN a.currency_code IN ('643', '810') THEN b.balance_out_rub ELSE 0 END) AS balance_out_rub,
            SUM(CASE WHEN a.currency_code NOT IN ('643', '810') THEN b.balance_out_rub ELSE 0 END) AS balance_out_val,
            SUM(b.balance_out_rub) AS balance_out_total
        FROM accounts a
        JOIN DM.DM_ACCOUNT_BALANCE_F b ON a.account_rk = b.account_rk
        WHERE b.on_date = v_ToDate
        GROUP BY a.ledger_account
    ),
	turnovers AS (
        SELECT 
            a.ledger_account,
            SUM(CASE WHEN a.currency_code IN ('643', '810') THEN t.debet_amount_rub ELSE 0 END) AS turn_deb_rub,
            SUM(CASE WHEN a.currency_code NOT IN ('643', '810') THEN t.debet_amount_rub ELSE 0 END) AS turn_deb_val,
            SUM(t.debet_amount_rub) AS turn_deb_total,
            SUM(CASE WHEN a.currency_code IN ('643', '810') THEN t.credit_amount_rub ELSE 0 END) AS turn_cre_rub,
            SUM(CASE WHEN a.currency_code NOT IN ('643', '810') THEN t.credit_amount_rub ELSE 0 END) AS turn_cre_val,
            SUM(t.credit_amount_rub) AS turn_cre_total
        FROM accounts a
        JOIN DM.DM_ACCOUNT_TURNOVER_F t ON a.account_rk = t.account_rk 
        WHERE t.on_date BETWEEN v_FromDate AND v_ToDate
        GROUP BY a.ledger_account
    )
    SELECT 
        v_FromDate,
        v_ToDate,
        s.chapter AS chapter,
        s.ledger_account AS ledger_account,
        s.characteristic AS characteristic,
        s.balance_in_rub AS balance_in_rub,
        s.balance_in_val AS balance_in_val,
        s.balance_in_total AS balance_in_total,
        COALESCE(t.turn_deb_rub, 0) AS turn_deb_rub,
        COALESCE(t.turn_deb_val, 0) AS turn_deb_val,
        COALESCE(t.turn_deb_total, 0) AS turn_deb_total,
        COALESCE(t.turn_cre_rub, 0) AS turn_cre_rub,
        COALESCE(t.turn_cre_val, 0) AS turn_cre_val,
        COALESCE(t.turn_cre_total, 0) AS turn_cre_total,
        COALESCE(e.balance_out_rub, 0) AS balance_out_rub,
        COALESCE(e.balance_out_val, 0) AS balance_out_val,
        COALESCE(e.balance_out_total, 0) AS balance_out_total
    FROM start_balances s
    FULL JOIN end_balances e ON s.ledger_account = e.ledger_account
    FULL JOIN turnovers t ON s.ledger_account = t.ledger_account; 
	
    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;

    v_end_time := clock_timestamp();

    IF v_rows_affected > 0 THEN
        INSERT INTO logs.proc_log (proc_name, on_date, start_time, end_time, row_count)
    VALUES (v_proc_name, i_OnDate, v_start_time, v_end_time, v_rows_affected);
    ELSE
        RAISE NOTICE 'Нет данных для даты %', i_OnDate;
    END IF;    

    COMMIT;
END;
$$;



-- проверка
SELECT * FROM DM.DM_F101_ROUND_F;
SELECT * FROM logs.proc_log ORDER BY log_id DESC; 
TRUNCATE TABLE dm.dm_f101_round_f;

CALL dm.fill_f101_round_f('2018-02-01');

SELECT * FROM logs.proc_log ORDER BY log_id DESC; 
SELECT * FROM DM.DM_F101_ROUND_F;
