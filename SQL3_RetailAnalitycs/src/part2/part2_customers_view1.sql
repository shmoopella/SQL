DROP VIEW IF EXISTS customers_view;


CREATE OR REPLACE VIEW customers_view AS
WITH union_tab AS (
    SELECT person_info.customer_id, transaction_summ, transaction_datetime, transaction_id, transaction_store_id
    FROM person_info
            JOIN cards
                  ON person_info.customer_id = cards.customer_id
            JOIN transactions
                  ON cards.customer_card_id = transactions.customer_card_id
    WHERE transaction_datetime <= (SELECT analysis_formation FROM date_of_analysis_formation)
),

avg_check AS (
    SELECT customer_id, AVG(transaction_summ) AS customer_avgerage_check
    FROM union_tab
    GROUP BY customer_id
    ORDER BY customer_avgerage_check DESC
),

avg_check_segment AS (
    SELECT *,
           CASE
               WHEN rn <= FLOOR(MAX(rn) over () * 0.1) THEN 'High'
               WHEN rn > FLOOR(MAX(rn) over () * 0.1) AND rn <= FLOOR(MAX(rn) over () * 0.35) THEN 'Medium'
               ELSE 'Low'
               END AS customer_average_check_segment
    FROM
         (SELECT *, ROW_NUMBER() OVER() AS rn
         FROM avg_check) AS rn
    GROUP BY customer_id, customer_avgerage_check, rn --без GROUP BY postgresql ругается, по этой же причине используется оконная функция, а не просто агрегатная
    ORDER BY customer_avgerage_check DESC
),

data_for_frequency AS (
     SELECT customer_id,
           MAX(transaction_datetime) AS late_date,
           MIN(transaction_datetime) AS early_date,
           COUNT(transaction_id) AS count_transactions
    FROM union_tab
    GROUP BY customer_id
),

frequency AS (
    SELECT customer_id,
                    EXTRACT(epoch FROM late_date - early_date) / 86400 / count_transactions AS customer_frequency
    FROM data_for_frequency
    ORDER BY customer_frequency ASC
),

frequency_segment AS (
    SELECT *,
           CASE
               WHEN rn <= FLOOR(MAX(rn) OVER () * 0.1) THEN 'Often'
               WHEN rn > FLOOR(MAX(rn) OVER () * 0.1) AND rn <= FLOOR(MAX(rn) OVER () * 0.35) THEN 'Occasionally'
               ELSE 'Rarely'
               END AS customer_frequency_segment
    FROM (SELECT *, ROW_NUMBER() OVER() AS rn
         FROM frequency) AS row_n_frequency
    GROUP BY customer_id, customer_frequency, rn
    ORDER BY customer_frequency ASC
),

inactive_period AS (
    SELECT data_for_frequency.customer_id,
           EXTRACT(epoch FROM (SELECT MAX(analysis_formation) FROM date_of_analysis_formation) - late_date) /
                 86400 AS customer_inactive_period
    FROM data_for_frequency
),

inactive_plus_churn_rate AS (
    SELECT inactive_period.customer_id, customer_inactive_period,
           CASE
               WHEN customer_frequency = 0 THEN NULL
               ELSE customer_inactive_period / freq.customer_frequency
           END AS customer_churn_rate
    FROM inactive_period
             JOIN frequency AS freq
                  ON inactive_period.customer_id = freq.customer_id
),

churn_segment AS (
    SELECT *,
           CASE
               WHEN customer_churn_rate >= 0 AND customer_churn_rate <= 2 THEN 'Low'
               WHEN customer_churn_rate > 2 AND customer_churn_rate <= 5 THEN 'Medium'
               WHEN customer_churn_rate > 5 THEN 'High'
               ELSE NULL
               END AS customer_churn_segment
    FROM inactive_plus_churn_rate
),

customer_segment AS (
    SELECT cs.customer_id,
           customer_average_check_segment,
           customer_frequency_segment,
           customer_churn_segment,
           segment_number.number AS customer_segment
    FROM avg_check_segment AS acs
             JOIN frequency_segment AS freq
                  ON acs.customer_id = freq.customer_id
             JOIN churn_segment AS cs
                  ON acs.customer_id = cs.customer_id
             JOIN segment_number
                  ON segment_number.frequency = customer_frequency_segment AND
                     segment_number.average_check = customer_average_check_segment AND
                     segment_number.churn = customer_churn_segment
),

top_store_by_rate AS (
SELECT *
FROM
    (SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id) AS rn
    FROM
    (SELECT customer_id, transaction_datetime, transaction_store_id,
          (COUNT(transaction_id) OVER (PARTITION BY customer_id, transaction_store_id))::numeric(20,2) /
           (COUNT(transaction_id) OVER (PARTITION BY customer_id))::numeric(20,2) AS trans_rate
    FROM union_tab
    ORDER BY customer_id ASC, trans_rate DESC, transaction_datetime DESC) AS res_t) r_t
   WHERE rn < 2
),

row_n_transactions AS (
    SELECT customer_id, transaction_store_id, transaction_datetime,
           ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY transaction_datetime DESC) AS rn
    FROM union_tab
),

top_store_by_last_three_trans AS (
    SELECT customer_id, MAX(primary_store) AS top_store_id
    FROM
        (SELECT customer_id,
               transaction_store_id,
               CASE
                   WHEN MAX(transaction_store_id) OVER (PARTITION BY customer_id) =
                       MIN(transaction_store_id) OVER (PARTITION BY customer_id) THEN transaction_store_id
                    ELSE 0
               END AS primary_store
        FROM (SELECT *
              FROM row_n_transactions
              WHERE rn < 4
              ORDER BY customer_id, transaction_store_id) AS res_t) AS search_store_by_three_trans
    GROUP BY customer_id
),

primary_store AS (
    SELECT top_store_by_rate.customer_id,
           CASE
               WHEN top_store_id = 0 THEN transaction_store_id
               ELSE top_store_id
               END AS customer_primary_store
    FROM top_store_by_rate
             JOIN top_store_by_last_three_trans
                  ON top_store_by_rate.customer_id = top_store_by_last_three_trans.customer_id
)

SELECT avg_check_segment.customer_id AS customer_id, customer_avgerage_check, avg_check_segment.customer_average_check_segment AS customer_avgerage_check_segment,
        customer_frequency, frequency_segment.customer_frequency_segment AS customer_frequency_segment, customer_inactive_period,
       customer_churn_rate, churn_segment.customer_churn_segment AS customer_churn_segment, customer_segment, customer_primary_store
FROM avg_check_segment
    LEFT JOIN frequency_segment
        ON avg_check_segment.customer_id = frequency_segment.customer_id
    LEFT JOIN churn_segment
        ON avg_check_segment.customer_id = churn_segment.customer_id
    LEFT JOIN customer_segment
        ON avg_check_segment.customer_id = customer_segment.customer_id
    LEFT JOIN primary_store
        ON avg_check_segment.customer_id = primary_store.customer_id;


SELECT *
FROM customers_view;