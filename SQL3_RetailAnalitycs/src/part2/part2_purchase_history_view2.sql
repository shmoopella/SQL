DROP VIEW IF EXISTS purchase_history;

CREATE VIEW purchase_history AS
WITH union_tab AS (
    SELECT person_info.customer_id AS customer_id, transactions.transaction_id AS transaction_id,
           transaction_datetime, transaction_summ,
           transactions.transaction_store_id, checks.sku_id AS sku_id, group_id, sku_amount, sku_summ,
           sku_summ_paid, sku_purchase_price, sku_amount * sku_purchase_price AS cost_price
    FROM person_info
             JOIN cards
                  ON person_info.customer_id = cards.customer_id
             JOIN transactions
                  ON cards.customer_card_id = transactions.customer_card_id
             JOIN checks
                  ON transactions.transaction_id = checks.transaction_id
             JOIN product_grid
                  ON checks.sku_id = product_grid.sku_id
            JOIN stores
                ON transactions.transaction_store_id = stores.transaction_store_id AND checks.sku_id = stores.sku_id
    WHERE transaction_datetime <= (SELECT analysis_formation FROM date_of_analysis_formation)
)

SELECT customer_id, transaction_id,
       transaction_datetime, group_id, SUM(cost_price) AS group_cost,
       SUM(sku_summ) AS group_summ, SUM(sku_summ_paid) AS group_summ_paid
FROM union_tab
GROUP BY customer_id, transaction_id, group_id, transaction_datetime;

SELECT *
FROM purchase_history;
