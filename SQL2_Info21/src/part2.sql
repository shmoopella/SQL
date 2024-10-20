--  1) процедура добавления P2P проверки

DROP PROCEDURE IF EXISTS add_p2p_check;

CREATE OR REPLACE PROCEDURE add_p2p_check (
       IN checker   VARCHAR,
       IN defender  VARCHAR,
       IN task_name VARCHAR,
       IN ch_status CHECK_STATUS,
       IN ch_time   TIME
       ) 
       AS $$
 BEGIN
           IF ch_status = 'Start' THEN
              INSERT INTO checks
              VALUES (
                     (SELECT MAX(id) + 1 FROM checks),
                     defender,
                     task_name,
                     CURRENT_DATE
                     );
          END IF;

       INSERT INTO p2p
       VALUES (
              (SELECT MAX(id) + 1 FROM p2p),
              (SELECT id FROM checks WHERE peer = defender AND task = task_name),
              checker,
              ch_status,
              ch_time
              );
   END;
$$ LANGUAGE plpgsql;

-- CALL add_p2p_check('kurdtko', 'iflet', 's21_math', 'Start', '03:50:00');
-- CALL add_p2p_check('kurdtko', 'iflet', 's21_math', 'Success', '03:52:34');


--  2) процедура добавления проверки Verter'ом

DROP PROCEDURE IF EXISTS add_verter_check;

CREATE OR REPLACE PROCEDURE add_verter_check (
       IN defender   VARCHAR,
       IN task_name  VARCHAR,
       IN ch_status  CHECK_STATUS,
       IN ch_time    TIME
       ) 
       AS $$
 BEGIN
       INSERT INTO verter
       VALUES (
              (SELECT MAX(id) + 1 FROM verter),
              (SELECT id FROM checks WHERE peer = defender AND task = task_name),
              ch_status,
              ch_time
              );
   END;
$$ LANGUAGE plpgsql;

-- CALL add_verter_check('iflet', 's21_math', 'Start', '03:55:01');
-- CALL add_verter_check('iflet', 's21_math', 'Success', '03:56:09');


--  3) триггер: после добавления записи со статутом "начало" в таблицу P2P
--     изменить соответствующую запись в таблице TransferredPoints

DROP TRIGGER IF EXISTS p2p_insert_trigger ON p2p;
DROP FUNCTION IF EXISTS update_transferred_points();

CREATE OR REPLACE FUNCTION update_transferred_points()
RETURNS TRIGGER AS $$
  BEGIN
         IF NEW.state = 'Start' THEN
            IF EXISTS (  -- если запись в TransferredPoints уже существует,
                      SELECT id
                        FROM transferredpoints AS tp
                       WHERE tp.checkingpeer = NEW.checkingpeer
                             AND tp.checkedpeer = (SELECT peer FROM checks WHERE id = NEW."check")
                      )
              THEN  -- то изменяем её
            UPDATE transferredpoints AS tp
               SET pointsamount = pointsamount + 1
             WHERE tp.checkingpeer = NEW.checkingpeer
                   AND tp.checkedpeer = (SELECT peer FROM checks WHERE id = NEW."check");
              ELSE  -- иначе создаём новую
                   INSERT INTO transferredpoints
                   VALUES (
                          (SELECT MAX(id) + 1 FROM transferredpoints),
                          NEW.checkingpeer,
                          (SELECT peer FROM checks WHERE id = NEW."check"),
                          1
                          );
                END IF;
        END IF;
        RETURN NEW;
    END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER p2p_insert_trigger
       AFTER INSERT ON p2p
       FOR EACH ROW
       EXECUTE FUNCTION update_transferred_points();

--INSERT INTO p2p
-- VALUES ((SELECT MAX(id) + 1 FROM p2p), 1, 'begibb', 'Start', '16:14:12');

-- INSERT INTO p2p
-- VALUES ((SELECT MAX(id) + 1 FROM p2p), 1, 'begibb', 'Start', '16:14:12');


--  4) триггер: перед добавлением записи в таблицу XP 
--     проверить корректность добавляемой записи

DROP TRIGGER IF EXISTS  xp_validation_trigger ON xp;
DROP FUNCTION IF EXISTS validate_xp();

CREATE OR REPLACE FUNCTION validate_xp()
RETURNS TRIGGER AS $$
DECLARE
        max_xp INTEGER;
        p2p_is_successed INTEGER;
        verter_is_successed INTEGER;
  BEGIN
        -- Узнаём максимальное количество XP для даннного задания
        SELECT maxxp
          INTO max_xp
          FROM tasks
         WHERE title = (SELECT task FROM checks WHERE id = NEW."check");

            IF NEW.xpamount > max_xp THEN
               RAISE EXCEPTION 'Превышено максимальное количество XP для даннного задания.';
           END IF;

        -- Проверяем, что поле Check ссылается на успешную проверку
        SELECT COUNT(*)
          INTO p2p_is_successed
          FROM p2p
         WHERE "check" = NEW."check"
               AND state = 'Success';

        SELECT COUNT(*)
          INTO verter_is_successed
          FROM verter
         WHERE "check" = NEW."check"
               AND state IN ('Success', NULL);

            IF p2p_is_successed + verter_is_successed = 0 THEN
               RAISE EXCEPTION 'Нельзя добавить XP за неуспешную проверку.';
           END IF;

        RETURN NEW;
    END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER xp_validation_trigger
       BEFORE INSERT ON xp
       FOR EACH ROW
       EXECUTE FUNCTION validate_xp();

-- INSERT INTO XP VALUES (6, 6, 301);  -- Вызовет exception

-- INSERT INTO xp 
-- VALUES (5, 6, 300),
--        (6, 9, 290);

-- INSERT INTO XP VALUES (7, 3, 100);  -- Вызовет exception
