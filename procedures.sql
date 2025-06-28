CREATE OR REPLACE PROCEDURE ds.fill_account_turnover_f(i_OnDate DATE)
LANGUAGE plpgsql
AS $$
BEGIN
    -- Очищаем витрину для указанной даты, если данные уже существовали
    DELETE FROM DM.DM_ACCOUNT_TURNOVER_F WHERE on_date = i_OnDate;
    
    -- Вставляем новые данные
    INSERT INTO DM.DM_ACCOUNT_TURNOVER_F (
        on_date,
        account_rk,
        credit_amount,
        credit_amount_rub,
        debet_amount,
        debet_amount_rub
    )
    WITH 
        -- Суммы по кредиту счетов
        credit_turnover AS (
            SELECT 
                credit_account_rk AS account_rk,
                SUM(credit_amount) AS credit_amount
            FROM ds.FT_POSTING_F
            WHERE oper_date = i_OnDate
            GROUP BY credit_account_rk
        ),
        
        -- Суммы по дебету счетов
        debet_turnover AS (
            SELECT 
                debet_account_rk AS account_rk,
                SUM(debet_amount) AS debet_amount
            FROM ds.FT_POSTING_F
            WHERE oper_date = i_OnDate
            GROUP BY debet_account_rk
        ),
        
        -- Курсы валют на указанную дату
        currency_rates AS (
            SELECT 
                currency_rk,
                reduced_cource AS rate
            FROM ds.MD_EXCHANGE_RATE_D
            WHERE data_actual_date <= i_OnDate 
              AND (data_actual_end_date IS NULL OR data_actual_end_date > i_OnDate)
        ) -- Закрывающая скобка добавлена здесь
        
    SELECT 
        i_OnDate AS on_date,
        acc.account_rk,
        COALESCE(ct.credit_amount, 0) AS credit_amount,
        COALESCE(ct.credit_amount, 0) * COALESCE(cr.rate, 1) AS credit_amount_rub,
        COALESCE(dt.debet_amount, 0) AS debet_amount,
        COALESCE(dt.debet_amount, 0) * COALESCE(cr.rate, 1) AS debet_amount_rub
    FROM 
        (SELECT DISTINCT account_rk FROM ds.MD_ACCOUNT_D) acc
    LEFT JOIN credit_turnover ct ON acc.account_rk = ct.account_rk
    LEFT JOIN debet_turnover dt ON acc.account_rk = dt.account_rk
    LEFT JOIN ds.MD_ACCOUNT_D a ON acc.account_rk = a.account_rk 
        AND a.data_actual_date <= i_OnDate 
        AND (a.data_actual_end_date IS NULL OR a.data_actual_end_date > i_OnDate)
    LEFT JOIN currency_rates cr ON a.currency_rk = cr.currency_rk
    WHERE (ct.credit_amount IS NOT NULL OR dt.debet_amount IS NOT NULL);
    
END;
$$;

-- Проверяем
CALL ds.fill_account_turnover_f('2018-01-18');

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

CALL ds.fill_account_turnover_period('2018-01-01', '2018-01-31');