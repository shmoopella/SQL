--TRUNCATE cards, checks, date_of_analysis_formation, person_info, product_grid, segment_number, sku_group, stores, transactions;

--Создание базы данных

CREATE DATABASE RetailAnalytics;

-- Создание таблиц

CREATE TABLE person_info (
	customer_id BIGINT,
	customer_name VARCHAR,
	customer_surname VARCHAR,
	customer_primary_email VARCHAR,
	customer_primary_phone VARCHAR,
	CONSTRAINT pk_person_info_customer_id PRIMARY KEY (customer_id)
);

CREATE TABLE cards (
	customer_card_id BIGINT,
	customer_id BIGINT,
	CONSTRAINT pk_cards_card_id PRIMARY KEY (customer_card_id),
	CONSTRAINT fk_cards_id FOREIGN KEY (customer_id) REFERENCES person_info (customer_id)
);

CREATE TABLE sku_group (
    group_id BIGINT,
    group_name VARCHAR,
    CONSTRAINT pk_sku_group_group_id PRIMARY KEY (group_id)

);

CREATE TABLE product_grid (
    sku_id BIGINT,
    sku_name VARCHAR,
    group_id BIGINT,
    CONSTRAINT pk_product_grip_sku_id PRIMARY KEY (sku_id),
    CONSTRAINT fk_product_grip_group_id FOREIGN KEY (group_id) REFERENCES sku_group(group_id)
);

CREATE TABLE stores (
    transaction_store_id BIGINT,
    sku_id BIGINT,
    sku_purchase_price NUMERIC,
    sku_retail_price NUMERIC,
    CONSTRAINT fk_stores_sku_id FOREIGN KEY (sku_id) REFERENCES product_grid(sku_id)
);

CREATE TABLE transactions (
	transaction_id BIGINT,
	customer_card_id BIGINT,
	transaction_summ NUMERIC,
	transaction_datetime TIMESTAMP,
	transaction_store_id BIGINT,
	CONSTRAINT pk_transactions_id PRIMARY KEY (transaction_id),
	CONSTRAINT fk_transactions_card_id FOREIGN KEY (customer_card_id) REFERENCES cards(customer_card_id)
);

CREATE TABLE checks (
    transaction_id BIGINT,
    sku_id BIGINT,
    sku_amount NUMERIC,
    sku_summ NUMERIC,
    sku_summ_paid NUMERIC,
    sku_discount NUMERIC,
    CONSTRAINT fk_checks_transaction_id FOREIGN KEY (transaction_id) REFERENCES transactions(transaction_id),
    CONSTRAINT fk_checks_sku_id FOREIGN KEY (sku_id) REFERENCES product_grid(sku_id)
);

CREATE TABLE date_of_analysis_formation (
    analysis_formation TIMESTAMP
);

CREATE TABLE segment_number ( --для view Customers из part 2
    number BIGINT,
    average_check VARCHAR,
    frequency VARCHAR,
    churn VARCHAR
);

-- Процедура импорта данных

DROP PROCEDURE IF EXISTS import_data(table_name VARCHAR, abs_path VARCHAR, delim VARCHAR(1));

CREATE OR REPLACE PROCEDURE import_data (IN table_name VARCHAR, IN abs_path VARCHAR, IN delim VARCHAR(1))
AS $$
BEGIN
    EXECUTE FORMAT ('COPY %s FROM %L DELIMITER %L', table_name, abs_path, delim);
END;
$$ LANGUAGE plpgsql;


-- Вызов процедуры импорта из csv файла

SET datestyle = 'ISO,DMY'; -- чтобы принимал формат даты из материалз
CALL import_data('person_info', '/Users/festusst/projects/SQL3_RetailAnalitycs_v1.0-3/datasets/Personal_Data_Mini.tsv', E'\t');
CALL import_data('cards', '/Users/festusst/projects/SQL3_RetailAnalitycs_v1.0-3/datasets/Cards_Mini.tsv', E'\t');
CALL import_data('sku_group', '/Users/festusst/projects/SQL3_RetailAnalitycs_v1.0-3/datasets/Groups_SKU_Mini.tsv', E'\t');
CALL import_data('product_grid', '/Users/festusst/projects/SQL3_RetailAnalitycs_v1.0-3/datasets/SKU_Mini.tsv', E'\t');
CALL import_data('stores', '/Users/festusst/projects/SQL3_RetailAnalitycs_v1.0-3/datasets/Stores_Mini.tsv', E'\t');
CALL import_data('transactions', '/Users/festusst/projects/SQL3_RetailAnalitycs_v1.0-3/datasets/Transactions_Mini.tsv', E'\t');
CALL import_data('date_of_analysis_formation', '/Users/festusst/projects/SQL3_RetailAnalitycs_v1.0-3/datasets/Date_Of_Analysis_Formation.tsv', E'\t');
CALL import_data('checks', '/Users/festusst/projects/SQL3_RetailAnalitycs_v1.0-3/datasets/Checks_Mini.tsv', E'\t');
CALL import_data('segment_number', '/Users/festusst/projects/SQL3_RetailAnalitycs_v1.0-3/src/part1/num_segment.tsv', E'\t');


-- Процедура экспорта данных

DROP PROCEDURE IF EXISTS export_data(table_name VARCHAR, abs_path VARCHAR, type VARCHAR(3));

CREATE OR REPLACE PROCEDURE export_data (IN table_name VARCHAR, IN abs_path VARCHAR, IN type VARCHAR(3))
AS $$
BEGIN
    IF type = 'csv'
    THEN
        EXECUTE FORMAT ('COPY %s TO %L CSV', table_name, abs_path);
    ELSEIF type = 'tsv'
    THEN
        EXECUTE FORMAT ('COPY %s TO %L', table_name, abs_path);
    ELSE
        RAISE EXCEPTION 'Error: type must be csv or tsv!';
    END IF;
END;
$$ LANGUAGE plpgsql;

--CALL export_data('person_info', '/Users/festusst/8.csv', 'csv');
--CALL export_data('person_info', '/Users/festusst/8.tsv', 'tsv');