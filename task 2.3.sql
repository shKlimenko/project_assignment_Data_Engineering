SELECT *   FROM dm.account_balance_turnover t1 ORDER BY t1.account_rk, t1.effective_date;

-- Подготовить запрос, который определит корректное значение поля account_in_sum. 
-- Если значения полей account_in_sum одного дня и account_out_sum предыдущего дня отличаются, 
-- то корректным выбирается значение account_out_sum предыдущего дня.
WITH corrected_values AS (
  SELECT 
    t1.account_rk,
    t1.currency_name,
    t1.department_rk,
    t1.effective_date,
    t1.account_out_sum,
    CASE 
      WHEN t1.account_in_sum != LAG(t1.account_out_sum) OVER (
        PARTITION BY t1.account_rk 
        ORDER BY t1.effective_date
      ) 
      AND LAG(t1.account_out_sum) OVER (
        PARTITION BY t1.account_rk 
        ORDER BY t1.effective_date
      ) IS NOT NULL
      THEN LAG(t1.account_out_sum) OVER (
        PARTITION BY t1.account_rk 
        ORDER BY t1.effective_date
      )
      ELSE t1.account_in_sum
    END AS corrected_account_in_sum
  FROM 
    dm.account_balance_turnover t1
)
SELECT 
  account_rk,
  currency_name,
  department_rk,
  effective_date,
  corrected_account_in_sum AS account_in_sum,
  account_out_sum
FROM 
  corrected_values;

-- Подготовить такой же запрос, только проблема теперь в том, что account_in_sum одного дня правильная, 
-- а account_out_sum предыдущего дня некорректна. Это означает, что если эти значения отличаются, 
-- то корректным значением для account_out_sum предыдущего дня выбирается значение account_in_sum текущего дня.
WITH corrected_values AS (
  SELECT 
    t1.account_rk,
    t1.currency_name,
    t1.department_rk,
    t1.effective_date,
    t1.account_in_sum,
    CASE 
      WHEN t1.account_out_sum != LEAD(t1.account_in_sum) OVER (
        PARTITION BY t1.account_rk 
        ORDER BY t1.effective_date
      ) 
      AND LEAD(t1.account_in_sum) OVER (
        PARTITION BY t1.account_rk 
        ORDER BY t1.effective_date
      ) IS NOT NULL
      THEN LEAD(t1.account_in_sum) OVER (
        PARTITION BY t1.account_rk 
        ORDER BY t1.effective_date
      )
      ELSE t1.account_out_sum
    END AS corrected_account_out_sum
  FROM 
    dm.account_balance_turnover t1
)
SELECT 
  account_rk,
  currency_name,
  department_rk,
  effective_date,
  account_in_sum,
  corrected_account_out_sum AS account_out_sum
FROM 
  corrected_values;



-- процедура заполнения витрины 'Изменение баланса счетов по дням'
CREATE OR REPLACE PROCEDURE dm.repair_account_balance_turnover()
LANGUAGE plpgsql
AS $$
BEGIN
	CREATE TEMP TABLE IF NOT EXISTS temp_corrected_values AS   
	WITH corrected_values AS (
	  SELECT 
	    t1.account_rk,
	    t1.currency_name,
	    t1.department_rk,
	    t1.effective_date,
	    t1.account_out_sum,
	    CASE 
	      WHEN t1.account_in_sum != LAG(t1.account_out_sum) OVER (
	        PARTITION BY t1.account_rk 
	        ORDER BY t1.effective_date
	      ) 
	      AND LAG(t1.account_out_sum) OVER (
	        PARTITION BY t1.account_rk 
	        ORDER BY t1.effective_date
	      ) IS NOT NULL
	      THEN LAG(t1.account_out_sum) OVER (
	        PARTITION BY t1.account_rk 
	        ORDER BY t1.effective_date
	      )
	      ELSE t1.account_in_sum
	    END AS corrected_account_in_sum
	  FROM 
	    dm.account_balance_turnover t1
	)
	SELECT 
	  account_rk,
	  currency_name,
	  department_rk,
	  effective_date,
	  corrected_account_in_sum AS account_in_sum,
	  account_out_sum
	FROM 
	  corrected_values;

    TRUNCATE TABLE dm.account_balance_turnover;

    INSERT INTO dm.account_balance_turnover (
			  account_rk,
			  currency_name,
			  department_rk,
			  effective_date,
			  account_in_sum,
			  account_out_sum)	
		SELECT account_rk,
			  currency_name,
			  department_rk,
			  effective_date,
			  account_in_sum,
			  account_out_sum
		FROM temp_corrected_values;
	
	RAISE NOTICE 'Данные в dm.account_balance_turnover успешно обновлены';
    
	EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            RAISE EXCEPTION 'Ошибка при обновлении dm.account_balance_turnover: %', SQLERRM;

	DROP TABLE IF EXISTS temp_corrected_values;
END;
$$;

CALL dm.repair_account_balance_turnover();