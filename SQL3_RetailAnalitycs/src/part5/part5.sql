create or replace function fnc_5 (period_dates varchar, num_transactions numeric, max_churn_index numeric, max_discount_share numeric, margin_share numeric)
RETURNS TABLE(Customer_ID bigint, Start_Date timestamp, End_Date timestamp, Required_Transactions_Count numeric, Group_Name varchar, Offer_Discount_Depth numeric)
language plpgsql
as
$$
DECLARE
    start_date date := split_part(period_dates, ' ', 1)::timestamp;
    end_date date := split_part(period_dates, ' ', 2)::timestamp;
BEGIN
    RETURN QUERY
    WITH step_1 as (SELECT customers_view.customer_id, start_date::timestamp, end_date::timestamp,
                           ((SELECT EXTRACT(EPOCH FROM (end_date::TIMESTAMP - start_date)) /
                                  customers_view.customer_frequency) + num_transactions)::numeric as Required_Transactions_Count
--                         ((SELECT extract( epoch from (end_date - start_date))
--                                      /customers_view.customer_frequency)::numeric + num_transactions) as Required_Transactions_Count
                 FROM customers_view)
        SELECT step_1.customer_id, step_1.start_date, step_1.end_date, step_1.Required_Transactions_Count, step_2.group_name, step_2.offer_discount_depth
        FROM step_1
    JOIN (SELECT * FROM determination_of_remuneration(max_churn_index, max_discount_share,margin_share)) step_2
    USING(customer_id);
END
$$;

SELECT * FROM fnc_5('2022-08-18 2022-08-18', 1, 3, 70, 30);
-- SELECT * FROM fnc_5('2022-08-11 2022-08-11', 1, 3, 70, 30);