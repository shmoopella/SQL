DROP VIEW IF EXISTS periods;

CREATE VIEW periods AS
WITH union_tab AS (
    SELECT person_info.customer_id AS customer_id, transactions.transaction_id AS transaction_id,
           transaction_datetime, transaction_summ,
           checks.sku_id AS sku_id, group_id, sku_amount, sku_summ, sku_discount,
           sku_summ_paid, transactions.transaction_store_id, sku_purchase_price, sku_retail_price
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
),
group_data AS (
    SELECT customer_id,
           group_id,
           MIN(transaction_datetime)      AS first_group_purchase_date,
           MAX(transaction_datetime)      AS last_group_purchase_date,
           COUNT(DISTINCT transaction_id) AS group_purchase
    FROM union_tab
    GROUP BY customer_id, group_id
),
group_frequency AS (
    SELECT *,
           (EXTRACT(epoch FROM last_group_purchase_date - first_group_purchase_date) / 86400 + 1) / group_purchase AS group_frequency
    FROM group_data
),
group_min_discount AS (
    SELECT customer_id, group_id, MIN(sku_discount / sku_summ) AS group_min_discount
    FROM union_tab
    WHERE sku_discount > 0
    GROUP BY customer_id, group_id
)

SELECT group_frequency.customer_id AS customer_id, group_frequency.group_id AS group_id,
        first_group_purchase_date, last_group_purchase_date, group_purchase,
        group_frequency, group_min_discount
FROM group_frequency
    JOIN group_min_discount
        ON group_frequency.customer_id = group_min_discount.customer_id AND group_frequency.group_id = group_min_discount.group_id;

SELECT * FROM periods;