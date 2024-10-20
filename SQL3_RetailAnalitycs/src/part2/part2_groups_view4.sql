DROP FUNCTION IF EXISTS margin;
DROP VIEW IF EXISTS groups;

CREATE VIEW groups AS
WITH union_tab AS (
    SELECT person_info.customer_id AS customer_id,
           checks.transaction_id   AS transaction_id, group_id,
           transaction_datetime, transaction_summ, transactions.transaction_store_id,
           checks.sku_id, sku_amount, sku_summ, sku_summ_paid, sku_discount
    FROM checks
             JOIN transactions
                  ON checks.transaction_id = transactions.transaction_id
             JOIN cards
                  ON transactions.customer_card_id = cards.customer_card_id
             JOIN person_info
                  ON cards.customer_id = person_info.customer_id
             JOIN product_grid
                  ON checks.sku_id = product_grid.sku_id
),
unique_sku_id AS (
    SELECT DISTINCT customer_id, sku_id
    FROM union_tab
    ORDER BY customer_id, sku_id
),
unique_group_id AS (
    SELECT DISTINCT customer_id, group_id
    FROM unique_sku_id
             JOIN product_grid
                  ON unique_sku_id.sku_id = product_grid.sku_id
    ORDER BY customer_id, group_id
),
purchase_history_with_group_dates AS (
    SELECT purchase_history.customer_id AS customer_id, transaction_id, transaction_datetime, periods.group_id,
           first_group_purchase_date, last_group_purchase_date
    FROM purchase_history
        JOIN periods
            ON purchase_history.customer_id = periods.customer_id
),
count_transactions_for_periods AS (
    SELECT customer_id, group_id, COUNT(transaction_id) AS count_transactions_for_periods
    FROM purchase_history_with_group_dates
    WHERE transaction_datetime >= first_group_purchase_date AND transaction_datetime <= last_group_purchase_date
    GROUP BY customer_id, group_id
),

group_affinity_index AS (
    SELECT ctfp.customer_id,
           ctfp.group_id,
           group_purchase::numeric / count_transactions_for_periods::numeric AS group_affinity_index
    FROM count_transactions_for_periods AS ctfp
             JOIN periods
                  ON ctfp.customer_id = periods.customer_id AND ctfp.group_id = periods.group_id
),
group_churn_rate AS (
    SELECT customer_id,
           group_id,
           count_days_for_last_transaction / group_frequency AS group_churn_rate
    FROM (SELECT periods.customer_id                                              AS customer_id,
                 periods.group_id                                                 AS group_id,
                 EXTRACT(epoch FROM ((SELECT analysis_formation FROM date_of_analysis_formation) -
                                           last_group_purchase_date)) / 86400 AS count_days_for_last_transaction,
                 group_frequency
          FROM unique_group_id
                   JOIN periods
                        ON unique_group_id.customer_id = periods.customer_id AND
                           unique_group_id.group_id = periods.group_id) AS count_days_for_last_transaction
),
period_diff AS (
    SELECT purchase_history.customer_id, purchase_history.group_id, group_frequency,
           EXTRACT(epoch FROM transaction_datetime -
           LAG(transaction_datetime) OVER (PARTITION BY purchase_history.customer_id, purchase_history.group_id ORDER BY transaction_datetime)) / 86400 AS period_diff
    FROM purchase_history
                   JOIN periods
                        ON purchase_history.customer_id = periods.customer_id AND
                           purchase_history.group_id = periods.group_id
),
deviation AS (
    SELECT *, abs_deviation / group_frequency AS relative_deviation
    FROM (SELECT *,
                 CASE
                     WHEN period_diff - group_frequency < 0 THEN (period_diff - group_frequency) * (-1)
                     ELSE period_diff - group_frequency
                     END AS abs_deviation
          FROM period_diff) AS abs_deviation
),
group_stability_index AS (
    SELECT customer_id, group_id, AVG(relative_deviation) AS group_stability_index
    FROM deviation
    GROUP BY customer_id, group_id
),
default_margin AS (
    SELECT customer_id, group_id, SUM(group_summ_paid) - SUM(group_cost) AS group_margin
    FROM purchase_history
    WHERE transaction_datetime <= (SELECT analysis_formation FROM date_of_analysis_formation)
    GROUP BY customer_id, group_id
),
period_margin AS (
    SELECT customer_id, group_id, SUM(group_summ_paid) - SUM(group_cost) AS group_margin
    FROM purchase_history
    WHERE transaction_datetime >= (SELECT (analysis_formation - INTERVAL '40 days') FROM date_of_analysis_formation) AND
          transaction_datetime <= (SELECT analysis_formation FROM date_of_analysis_formation)
    GROUP BY customer_id, group_id
),
transaction_margin AS (
    SELECT customer_id, group_id, SUM(group_summ_paid) - SUM(group_cost) AS group_margin
    FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY transaction_datetime DESC) AS rn
          FROM purchase_history) AS r_n
    WHERE rn <= 3
    GROUP BY customer_id, group_id
),
group_discounts AS (
    SELECT periods.customer_id AS customer_id, periods.group_id AS group_id,
           count_discount_trans::NUMERIC / group_purchase::NUMERIC AS group_discount_share,
           group_min_discount AS group_minimum_discount, group_average_discount
    FROM periods
        LEFT JOIN (SELECT customer_id, group_id, COUNT(transaction_id) AS count_discount_trans
                  FROM union_tab
                  WHERE sku_discount > 0
                  GROUP BY customer_id, group_id) AS discount_trans
            ON discount_trans.customer_id = periods.customer_id AND discount_trans.group_id = periods.group_id
        LEFT JOIN (SELECT customer_id, group_id, SUM(group_summ_paid) / SUM(group_summ) AS group_average_discount
                    FROM purchase_history
                    WHERE group_summ_paid < group_summ
                    GROUP BY customer_id, group_id) AS avg_discount
                ON discount_trans.customer_id = avg_discount.customer_id AND discount_trans.group_id = avg_discount.group_id
)
SELECT *
FROM unique_group_id
    JOIN group_affinity_index
        USING (customer_id, group_id)
    JOIN group_churn_rate
        USING(customer_id, group_id)
    JOIN group_stability_index
        USING(customer_id, group_id)
    JOIN group_discounts
        USING(customer_id, group_id);


CREATE OR REPLACE FUNCTION groups_with_margin (IN method VARCHAR DEFAULT 'all_transactions', IN count_days INTERVAL DEFAULT '200 days',
                                    IN count_transactions BIGINT DEFAULT 100)
RETURNS TABLE (customer_id BIGINT, group_id  BIGINT, group_affinity_index NUMERIC, group_churn_rate NUMERIC,group_stability_index NUMERIC,
               group_discount_share NUMERIC, group_minimum_discount NUMERIC, group_average_discount NUMERIC, group_margin NUMERIC)
AS $$
    BEGIN
        IF method = 'period'
            THEN RETURN QUERY
            EXECUTE
            '
            SELECT *
            FROM groups
                LEFT JOIN (SELECT purchase_history.customer_id, purchase_history.group_id, SUM(group_summ_paid) - SUM(group_cost) AS group_margin
                       FROM purchase_history
                       WHERE transaction_datetime >= (SELECT (analysis_formation - $1) FROM date_of_analysis_formation) AND
                                transaction_datetime <= (SELECT analysis_formation FROM date_of_analysis_formation)
                       GROUP BY customer_id, group_id) AS period_margin
                USING(customer_id, group_id)
            '
        USING count_days;
        ElSEIF method = 'transaction'
            THEN
                RETURN QUERY
                EXECUTE
            '
            SELECT *
            FROM groups
                LEFT JOIN
                        (SELECT customer_id, group_id, SUM(group_summ_paid) - SUM(group_cost) AS group_margin
                        FROM
                            (SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY transaction_datetime DESC) AS rn
                            FROM purchase_history) AS r_n
                        WHERE rn <= $1
                        GROUP BY customer_id, group_id) AS trans_margin
                USING(customer_id, group_id)
            '
            USING count_transactions;
        ELSE
            RETURN QUERY
            SELECT *
            FROM groups
                LEFT JOIN (SELECT purchase_history.customer_id, purchase_history.group_id,
                                  SUM(group_summ_paid) - SUM(group_cost) AS group_margin
                       FROM purchase_history
                       GROUP BY purchase_history.customer_id, purchase_history.group_id) AS defaul_margin
                USING(customer_id, group_id);
        END IF;
    END;
$$ LANGUAGE plpgsql;


--расчет по дефолту
SELECT * FROM groups_with_margin();


-- расчет по периоду
--SELECT * FROM groups_with_margin('period', '200 days', '0');


-- расчет по трансакциям

--SELECT * FROM groups_with_margin('transaction', '0', '200');
