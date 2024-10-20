DROP FUNCTION IF EXISTS Cross_Selling;
DROP TABLE IF EXISTS temp_tab;

CREATE TEMPORARY TABLE temp_tab AS SELECT transactions.Transaction_id, checks.SKU_ID, sku_group.Group_ID
                                    FROM transactions
                                        JOIN checks ON transactions.transaction_id = checks.transaction_id
                                        JOIN product_grid ON checks.sku_id = product_grid.sku_id
                                        JOIN sku_group ON product_grid.group_id = sku_group.group_id;

CREATE OR REPLACE FUNCTION Cross_Selling (IN Groups_Amount INTEGER,
                                          IN Max_Group_Churn_Rate NUMERIC,
                                          IN Max_Group_Stability_Index NUMERIC,
                                          IN Max_SKU_Share NUMERIC,
                                          IN Allowed_Margin_Share NUMERIC)
RETURNS TABLE (Customer_ID BIGINT,
               SKU_Name VARCHAR,
               Offer_Discount_Depth NUMERIC)
LANGUAGE plpgsql
AS $function$
    BEGIN
        RETURN QUERY WITH Group_Selection AS (SELECT temp1.customer_id, temp1.group_id, customer_primary_store
                                              FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY groups.customer_id ORDER BY groups.group_affinity_index DESC) AS Amount
                                              FROM groups WHERE groups.group_churn_rate <= Max_Group_Churn_Rate
                                                            AND groups.group_stability_index < Max_Group_Stability_Index) AS temp1
                                                  JOIN customers_view ON customers_view.customer_id = temp1.customer_id WHERE Amount <= Groups_Amount),

        SKU_Max_Margin AS (SELECT Group_Selection.customer_id,
                                  Group_Selection.group_id,
                                  Group_Selection.customer_primary_store,
                                  temp2.sku_id,
                                  temp2.margin,
                                  temp2.sku_retail_price
                            FROM Group_Selection
                                JOIN (SELECT stores.transaction_store_id,
                                             stores.sku_id,
                                             stores.sku_retail_price,
                                             stores.sku_retail_price - stores.sku_purchase_price AS margin,
                                             product_grid.group_id,
                                             ROW_NUMBER() OVER (PARTITION BY stores.transaction_store_id, product_grid.group_id
                                                 ORDER BY (stores.sku_retail_price - stores.sku_purchase_price) DESC) AS Amount
                                             FROM stores
                                                 JOIN product_grid ON stores.sku_id = product_grid.sku_id) AS temp2
                                                 ON Group_Selection.group_id = temp2.group_id AND Group_Selection.customer_primary_store = temp2.transaction_store_id WHERE temp2.Amount = 1),

        SKU_Shares AS (SELECT Un_Trans.group_id, Un_Trans.sku_id, Un_Trans.transaction::NUMERIC / Trans_Sum.gr AS Share
                       FROM (SELECT group_id, sku_id, COUNT(DISTINCT transaction_id) AS transaction
                       FROM temp_tab GROUP BY group_id, sku_id) AS Un_Trans
                            JOIN (SELECT group_id, COUNT(DISTINCT transaction_id) AS gr FROM temp_tab GROUP BY group_id) AS Trans_Sum
                            ON Un_Trans.group_id = Trans_Sum.group_id),

        Discount_Calculation AS (SELECT SKU_Max_Margin.customer_id,
                                        SKU_Max_Margin.group_id,
                                        SKU_Max_Margin.customer_primary_store,
                                        SKU_Max_Margin.sku_id,
                                        (SKU_Max_Margin.margin * Allowed_Margin_Share::NUMERIC / 100) / SKU_Max_Margin.sku_retail_price AS discount,
                                        CEIL(periods.group_min_discount / 0.05) * 0.05 AS min_discount
                                 FROM SKU_Max_Margin
                                     JOIN periods ON SKU_Max_Margin.customer_id = periods.customer_id AND SKU_Max_Margin.group_id = periods.group_id
                                     JOIN SKU_Shares ON SKU_Max_Margin.sku_id = SKU_Shares.sku_id AND SKU_Max_Margin.group_id = SKU_Shares.group_id
                                     WHERE SKU_Shares.Share <= Max_Sku_Share::NUMERIC / 100)

        SELECT Discount_Calculation.customer_id, product_grid.sku_name, Discount_Calculation.min_discount * 100 AS Offer_Discount_Depth
            FROM Discount_Calculation
                JOIN product_grid ON Discount_Calculation.sku_id = product_grid.sku_id WHERE discount >= min_discount;
    END
$function$;

-- TEST
-- SELECT * FROM Cross_Selling(10, 10, 1, 100, 100);
SELECT * FROM Cross_Selling(5, 3, 0.5, 100, 30);
