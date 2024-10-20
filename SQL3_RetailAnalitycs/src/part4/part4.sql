create or replace function fnc_for_method_1(parameter_for_method varchar, coeff_increase_avrg_check numeric)
returns TABLE(Customer_ID bigint, Required_Check_Measure numeric) language plpgsql
as $$
DECLARE
    start_date date := split_part(parameter_for_method, ' ', 1)::timestamp;
    end_date date := split_part(parameter_for_method, ' ', 2)::timestamp;
    min_date date := (SELECT transaction_datetime::timestamp FROM transactions ORDER BY 1 LIMIT 1);
    max_date date := (SELECT transaction_datetime::timestamp FROM transactions ORDER BY 1 DESC LIMIT 1);
BEGIN
    IF (start_date > end_date) THEN
    RAISE EXCEPTION 'Дата начала периода не может быть позже даты конца периода';
    END IF;
    IF (end_date < start_date) THEN
    RAISE EXCEPTION 'Дата конца периода не может быть раньше даты начала периода';
    END IF;
    IF (start_date < min_date) THEN start_date = min_date;
    END IF;
    IF (end_date > max_date) THEN end_date = max_date;
    END IF;
    RETURN QUERY
        WITH tmp AS (SELECT c.customer_id, t.transaction_summ AS transaction_summ
            FROM cards c
            JOIN transactions t using(customer_card_id)
            WHERE t.transaction_datetime BETWEEN start_date AND end_date)
            SELECT DISTINCT tmp.customer_id, (avg(transaction_summ) over(partition by tmp.customer_id ))::numeric * coeff_increase_avrg_check as Required_Check_Measure
            FROM tmp;
END
$$;


create OR REPLACE function fnc_for_method_2(parameter_for_method varchar, coeff_increase_avrg_check numeric)
    returns TABLE(C_ID bigint, Required_Check_Measure numeric)
    language plpgsql
as $$
DECLARE
    num_transactions int := (SELECT CAST(parameter_for_method AS int));
BEGIN
    RETURN QUERY
        WITH query AS (SELECT customer_id, customer_card_id, transaction_summ, rank() OVER (PARTITION BY c.customer_id ORDER BY transaction_datetime DESC) AS rank
                    FROM transactions
                    JOIN cards c using(customer_card_id)),
            tmp as (select query.customer_id, customer_card_id, transaction_summ
                    from query
                    where rank <= num_transactions)
        SELECT tmp.customer_id, (avg(tmp.transaction_summ) OVER (PARTITION BY tmp.customer_id))::numeric * coeff_increase_avrg_check AS Required_Check_Measure
        FROM tmp;
END;
$$;

-- drop function determination_of_remuneration(numeric, numeric, numeric)

CREATE VIEW groups_with_m AS
WITH  m as (SELECT purchase_history.customer_id, purchase_history.group_id,
                    AVG(group_summ_paid - group_cost) AS group_margin
                    FROM purchase_history
                    JOIN groups g USING(customer_id, group_id)
                    GROUP BY purchase_history.customer_id, purchase_history.group_id)
SELECT groups.customer_id, groups.group_id, group_affinity_index,
                group_churn_rate, group_discount_share, group_minimum_discount, m.group_margin,
                dense_rank() OVER (PARTITION BY groups.customer_id ORDER BY group_affinity_index DESC) as row
FROM groups
JOIN m USING(customer_id, group_id)
ORDER BY customer_id, row;

create or replace function determination_of_remuneration (max_churn_index numeric, max_discount_share numeric, margin_share numeric)
RETURNS TABLE (Customer_ID bigint, Group_Name varchar, Offer_Discount_Depth numeric)
    language plpgsql
as
$$
DECLARE queue record;
        discount numeric;
        flag_id int := 0;
BEGIN
    FOR queue in (SELECT * FROM groups_with_m
                WHERE  group_churn_rate <= max_churn_index AND group_discount_share < (max_discount_share / 100.)
                    ORDER BY customer_id, group_minimum_discount)
    LOOP
    discount := (floor((queue.group_minimum_discount * 100 / 5)) * 5);
    IF (flag_id != queue.customer_id) THEN
        IF (queue.group_margin > 0 AND queue.group_minimum_discount::numeric(10, 2) > 0
            AND queue.group_margin * margin_share / 100.0 > discount * queue.group_margin / 100.0) THEN
            flag_id = queue.customer_id;
            RETURN QUERY
                SELECT distinct ON (g.customer_id) g.customer_id, sku_group.group_name,
                                                   (CASE WHEN discount = 0 THEN discount + 5
                                                       ELSE discount
                                                    END) AS Offer_Discount_Depth
                FROM groups g
                    JOIN sku_group USING(group_id)
                WHERE queue.customer_id = g.customer_id AND queue.group_id = g.group_id;
        END IF;
    END IF;
END LOOP;
END;
$$;

create or replace function fnc_4(calc_method int, parameter_for_method varchar, coeff_increase_avrg_check numeric,
    max_churn_index numeric, max_discount_share numeric, margin_share numeric)
RETURNS TABLE (Customer_ID bigint, Required_Check_Measure numeric,
Group_Name varchar, Offer_Discount_Depth numeric) language plpgsql
as
$$
BEGIN
IF calc_method = 1 THEN
    RETURN QUERY
    SELECT step_1.Customer_ID, round(step_1.Required_Check_Measure, 2), step_2.Group_Name, step_2.Offer_Discount_Depth
    FROM (select * from fnc_for_method_1(parameter_for_method, coeff_increase_avrg_check) ) step_1
    JOIN (select * from determination_of_remuneration(max_churn_index, max_discount_share, margin_share)) step_2
    ON step_1.Customer_ID = step_2.Customer_ID;
ELSEIF calc_method = 2 THEN
    RETURN QUERY
    SELECT step_1.C_ID as Customer_ID, round(step_1.Required_Check_Measure, 2), step_2.Group_Name, step_2.Offer_Discount_Depth
    FROM (select distinct * from fnc_for_method_2(parameter_for_method, coeff_increase_avrg_check))as step_1
    JOIN (select distinct * from determination_of_remuneration(max_churn_index, max_discount_share, margin_share)) as step_2
    ON step_1.C_ID = step_2.Customer_ID;
ELSE
    RAISE EXCEPTION 'Значеие метода может быть только 1 или 2';
END IF;
END
$$;

SELECT * from fnc_4(2, '100',  1.15, 3, 70, 30);
-- SELECT * from fnc_4(1, '2020-10-10 2022-10-10',  1.15, 3, 70, 30);
-- select * from determination_of_remuneration(3, 70, 30);
