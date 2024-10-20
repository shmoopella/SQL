-- CREATE DATABASE meta;

CREATE TABLE "TableName_1" (
    column_1 VARCHAR,
    column_2 INT
);

CREATE TABLE "TableName_2" (
    column_1 VARCHAR,
    column_2 INT
);

CREATE TABLE "AnotherTable_1" (
    column_1 VARCHAR,
    column_2 INT
);

CREATE TABLE "AnotherTable_2" (
    column_1 VARCHAR,
    column_2 INT
);


-- 1) Создать хранимую процедуру, которая, не уничтожая базу данных, 
-- уничтожает все те таблицы текущей базы данных, имена которых 
-- начинаются с фразы 'TableName'.

 CREATE OR REPLACE PROCEDURE drop_tables(
        IN tname VARCHAR DEFAULT 'TableName'
) AS $$
DECLARE
        rec RECORD;
  BEGIN
         FOR rec IN 
             SELECT table_name
               FROM information_schema.tables
              WHERE table_catalog = current_database()
                    AND table_schema NOT IN ('information_schema', 'pg_catalog')
                    AND table_name LIKE tname || '%'
        LOOP
            EXECUTE 'DROP TABLE ' || quote_ident(rec.table_name) || ' CASCADE ';
            RAISE INFO 'Dropped table: %', quote_ident(rec.table_name);
        END LOOP;
END;
$$
LANGUAGE plpgsql;

CALL drop_tables();


--  2) Создать хранимую процедуру с выходным параметром, которая 
-- выводит список имен и параметров всех скалярных SQL функций 
-- пользователя в текущей базе данных. Имена функций без параметров
-- не выводить. Имена и список параметров должны выводиться в одну 
-- строку. Выходной параметр возвращает количество найденных функций.


-- функции-пустышки для демонстрации работы get_all_parameterized_functions():

-- без аргументов
 CREATE OR REPLACE FUNCTION function_without_args()
RETURNS VOID
AS $$
  BEGIN
        -- ...
    END;
$$
LANGUAGE plpgsql;

-- с одним аргументом
 CREATE OR REPLACE FUNCTION function_with_one_arg(
        IN some_arg TEXT
        )
RETURNS VOID
AS $$
  BEGIN
        -- ...
    END;
$$
LANGUAGE plpgsql;

-- с двумя аргументами
 CREATE OR REPLACE FUNCTION function_with_two_args(
        IN arg_1 TEXT,
        IN arg_2 INT
        )
RETURNS VOID
AS $$
  BEGIN
        -- ...
    END;
$$
LANGUAGE plpgsql;


-- основная функция 
 CREATE OR REPLACE FUNCTION get_all_parameterized_functions() 
RETURNS TABLE (
        "Function name" TEXT,
        "Arguments"     TEXT
) AS $$
  BEGIN
        RETURN QUERY
        SELECT proname::TEXT, array_to_string(proargnames, ', ')
          FROM pg_proc
         WHERE prokind = 'f'  -- f - скалярные фунции
               AND pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public')
               AND proargnames IS NOT NULL
               AND proretset = false  -- только функции, возвращающие единственное значение
               AND prorettype != 'pg_catalog.trigger'::regtype;  -- исключаем функции, возвращающие триггеры
    END;
$$
LANGUAGE plpgsql;

-- основная процедура
 CREATE OR REPLACE PROCEDURE count_all_parameterized_functions(
        OUT amount INT
) AS $$
  BEGIN
        SELECT COUNT(*)
          FROM get_all_parameterized_functions()
          INTO amount;
    END;
$$
LANGUAGE plpgsql;

SELECT * FROM get_all_parameterized_functions();
CALL count_all_parameterized_functions(amount := 0);


-- 3) Создать хранимую процедуру с выходным параметром, которая уничтожает
-- все SQL DML триггеры в текущей базе данных. Выходной параметр возвращает 
-- количество уничтоженных триггеров.


-- функция-пустышка для демонстрации работы drop_all_triggers():
 CREATE OR REPLACE FUNCTION function_which_returns_trigger()
RETURNS TRIGGER AS $$
  BEGIN
        RETURN NEW;
    END;
$$ LANGUAGE plpgsql;


-- триггеры-пустышки для демонстрации работы drop_all_triggers():

CREATE OR REPLACE TRIGGER trigger_1
       AFTER INSERT ON "AnotherTable_1"
       FOR EACH ROW
       EXECUTE FUNCTION function_which_returns_trigger();

CREATE OR REPLACE TRIGGER trigger_2
       AFTER INSERT ON "AnotherTable_2"
       FOR EACH ROW
       EXECUTE FUNCTION function_which_returns_trigger();


-- основная процедура
CREATE OR REPLACE PROCEDURE drop_all_triggers(
        OUT amount INT
) AS $$
DECLARE
        r record;
  BEGIN
        SELECT 0 INTO amount;

        BEGIN
               FOR r IN 
                    SELECT trigger_name, event_object_table
                      FROM information_schema.triggers
                     WHERE trigger_catalog = current_database()
              LOOP
                   EXECUTE 'DROP TRIGGER IF EXISTS ' || r.trigger_name || ' ON "' || r.event_object_table || '" CASCADE;';
                    SELECT amount + 1 INTO amount;
              END LOOP;
          END;
END;
$$
LANGUAGE plpgsql;

CALL drop_all_triggers(amount := 0);


-- 4) Создать хранимую процедуру с входным параметром, которая выводит
-- имена и описания типа объектов (только хранимых процедур и скалярных 
-- функций), в тексте которых на языке SQL встречается строка, задаваемая 
-- параметром процедуры.

 CREATE OR REPLACE FUNCTION find_function_by_substring(
        IN substr TEXT
        )
RETURNS TABLE (
        "Name" TEXT,
        "Type of object" TEXT
        )
AS $$
  BEGIN
        RETURN QUERY
        SELECT proname::TEXT,
               CASE WHEN prokind = 'f' 
                    THEN 'Scalar function'
                    ELSE 'Procedure'
                END
          FROM pg_proc
         WHERE pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public')
               AND prokind IN ('f', 'p')
               AND prosrc LIKE '%' || substr || '%';
    END;
$$ LANGUAGE plpgsql;

SELECT * FROM find_function_by_substring('...');