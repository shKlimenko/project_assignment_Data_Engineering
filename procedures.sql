-- процедура с логгированием
CREATE OR REPLACE PROCEDURE ds.fill_account_turnover_f(i_OnDate DATE)
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc_name TEXT := 'ds.fill_account_turnover_f';
    v_start_time TIMESTAMP;
    v_end_time   TIMESTAMP;
    v_rows_affected INT := 0;
BEGIN
    -- Фиксируем время начала
    v_start_time := clock_timestamp();
   
    -- Очищаем витрину для указанной даты
    DELETE FROM DM.DM_ACCOUNT_TURNOVER_F WHERE on_date = i_OnDate;

    -- Вставляем новые данные
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
    WHERE (ct.credit_amount IS NOT NULL OR dt.debet_amount IS NOT NULL);

    -- Получаем количество вставленных строк
    GET DIAGNOSTICS v_rows_affected = ROW_COUNT;

    -- Фиксируем время завершения
    v_end_time := clock_timestamp();


    -- Логируем только если были вставлены данные
    IF v_rows_affected > 0 THEN
        INSERT INTO logs.proc_log (proc_name, on_date, start_time, end_time, row_count)
    VALUES (v_proc_name, i_OnDate, v_start_time, v_end_time, v_rows_affected);
    ELSE
        RAISE NOTICE 'Нет данных для даты %', i_OnDate;
    END IF;

END;
$$;

-- Проверяем
CALL ds.fill_account_turnover_f('2018-01-18');

-- процедура с периодом
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

-- Проверяем
CALL ds.fill_account_turnover_period('2018-01-01', '2018-01-31');






-- процедура заполнения витрины остатков 2017-12-31
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
        currency_rk,
    	balance_out,
        balance_out_rub
    )
    SELECT 
        b.on_date,
        b.account_rk,
        b.currency_rk,
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

-- Проверяем
CALL ds.fill_account_balance_2017_12_31_f();



-- процедура заполнения витрины остатков
CREATE OR REPLACE PROCEDURE ds.fill_account_balance_f(i_OnDate DATE)
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc_name TEXT := 'ds.fill_account_balance2_f';
    v_start_time TIMESTAMP;
    v_end_time   TIMESTAMP;
    v_prev_date DATE := i_OnDate - INTERVAL '1 day';
    v_rows_affected INT := 0;
BEGIN
    v_start_time := clock_timestamp();

    DELETE FROM DM.DM_ACCOUNT_BALANCE_F WHERE on_date = i_OnDate;

    INSERT INTO DM.DM_ACCOUNT_BALANCE_F (
        on_date,
        account_rk,
        currency_rk,
		balance_out,
        balance_out_rub
    )
    SELECT 
        i_OnDate AS on_date,
        a.account_rk,
        a.currency_rk,
        CASE 
            WHEN a.char_type = 'А' THEN COALESCE(b.balance_out, 0) + COALESCE(t.debet_amount, 0) - COALESCE(t.credit_amount, 0)
            WHEN a.char_type = 'П' THEN COALESCE(b.balance_out, 0) - COALESCE(t.debet_amount, 0) + COALESCE(t.credit_amount, 0)
        END AS balance_out,
        CASE 
            WHEN a.char_type = 'А' THEN COALESCE(b.balance_out_rub, 0) + COALESCE(t.debet_amount_rub, 0) - COALESCE(t.credit_amount_rub, 0)
            WHEN a.char_type = 'П' THEN COALESCE(b.balance_out_rub, 0) - COALESCE(t.debet_amount_rub, 0) + COALESCE(t.credit_amount_rub, 0)
        END AS balance_out_rub
    FROM (
        SELECT DISTINCT account_rk, currency_rk, char_type
        FROM ds.MD_ACCOUNT_D
        WHERE data_actual_date <= i_OnDate
          AND (data_actual_end_date IS NULL OR data_actual_end_date > i_OnDate)
          AND char_type IN ('А', 'П')
    ) a
    LEFT JOIN (
        SELECT account_rk, balance_out, balance_out_rub
        FROM DM.DM_ACCOUNT_BALANCE_F
        WHERE on_date = v_prev_date
    ) b ON a.account_rk = b.account_rk
    LEFT JOIN (
        SELECT account_rk, credit_amount, debet_amount, credit_amount_rub, debet_amount_rub
        FROM DM.DM_ACCOUNT_TURNOVER_F
        WHERE on_date = i_OnDate
    ) t ON a.account_rk = t.account_rk;

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


-- процедура заполнения витриныостатков с периодом
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


-- Проверяем
CALL ds.fill_account_balance_period('2018-01-01', '2018-01-31');





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
