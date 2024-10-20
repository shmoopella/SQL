-- 1) Написать функцию, возвращающую таблицу TransferredPoints в более человекочитаемом виде

DROP FUNCTION IF EXISTS get_transferred_points_human_readable();

CREATE OR REPLACE FUNCTION get_transferred_points_human_readable() 
RETURNS TABLE (
        peer1 VARCHAR,
        peer2 VARCHAR,
        pointsamount BIGINT
) AS $$
  BEGIN
        RETURN QUERY
        SELECT checkingpeer,
               checkedpeer,
               CASE 
                    WHEN
                         (
                         SELECT tp2.pointsamount
                           FROM transferredpoints AS tp2
                          WHERE checkingpeer = tp1.checkedpeer
                                AND checkedpeer = tp1.checkingpeer
                          ) > tp1.pointsamount
                     THEN 
                          tp1.pointsamount * -1
                     ELSE
                          tp1.pointsamount
                END
           FROM transferredpoints AS tp1;
    END;
$$ LANGUAGE plpgsql;

-- SELECT * FROM get_transferred_points_human_readable();


-- Версия, которая возвращает разницу

--  CREATE OR REPLACE FUNCTION get_transferred_points_human_readable() 
-- RETURNS TABLE (
--         peer1 VARCHAR,
--         peer2 VARCHAR,
--         pointsamount BIGINT
-- ) AS $$
--   BEGIN
--         RETURN QUERY
--         SELECT checkingpeer,
--                checkedpeer, 
--                tp1.pointsamount - COALESCE(
--                   (SELECT tp2.pointsamount
--                     FROM transferredpoints AS tp2
--                    WHERE checkingpeer = tp1.checkedpeer
--                          AND checkedpeer = tp1.checkingpeer)
--                   , 0)
--           FROM transferredpoints AS tp1;
--     END;
-- $$ LANGUAGE plpgsql;

-- SELECT * FROM get_transferred_points_human_readable();


-- 2) Написать функцию, которая возвращает таблицу вида: ник пользователя, название проверенного задания, кол-во полученного XP

DROP FUNCTION IF EXISTS success_tasks_and_xp();

CREATE OR REPLACE FUNCTION success_tasks_and_xp() RETURNS TABLE (peer VARCHAR, task VARCHAR, xp BIGINT) AS
$$
    SELECT peer, split_part(task, '_', 1) AS task, xpamount AS xp
    FROM checks JOIN p2p
    ON checks.id = p2p."check"
    LEFT JOIN verter
    ON checks.id = verter."check"
    JOIN xp
    ON checks.id = xp."check"
    WHERE p2p.state = 'Success' AND (verter.state = 'Success' OR verter.state IS NULL);
$$LANGUAGE sql;

-- SELECT * FROM success_tasks_and_xp();


-- 3) Написать функцию, определяющую пиров, которые не выходили из кампуса в течение всего дня

DROP FUNCTION IF EXISTS peers_have_no_left_campus(IN "date" date);

CREATE OR REPLACE FUNCTION peers_have_no_left_campus(IN "date" date)
RETURNS SETOF varchar AS
$$
SELECT peer
FROM timetracking
GROUP BY peer, timetracking.date
HAVING COUNT(state) = 1 OR COUNT(state) = 2 AND timetracking.date = $1;
$$ LANGUAGE sql;

-- SELECT * FROM peers_have_no_left_campus('2023-03-11');


-- 4) Найти процент успешных и неуспешных проверок за всё время

DROP PROCEDURE IF EXISTS get_successful_checks;

CREATE OR REPLACE PROCEDURE get_successful_checks(
        OUT successfulchecks   INTEGER,
        OUT unsuccessfulchecks INTEGER
) AS $$
DECLARE
        all_checks INTEGER;
  BEGIN
        SELECT COUNT(*)
          INTO all_checks
          FROM verter
         WHERE state IN ('Success', 'Failure');

        SELECT (
               (SELECT COUNT(*)
                  FROM verter
                 WHERE state = 'Success'
               )
               * 100 / all_checks
               ) INTO successfulchecks;

        SELECT (
               (SELECT COUNT(*)
                 FROM verter
                WHERE state = 'Failure'
               )
               * 100 / all_checks
               ) INTO unsuccessfulchecks;
    END;
$$ LANGUAGE plpgsql;

--CALL get_successful_checks(successfulchecks := 0, unsuccessfulchecks := 0);


-- 5) Посчитать изменение в количестве пир поинтов каждого пира по таблице TransferredPoints

DROP FUNCTION IF EXISTS calculate_changed_points();

CREATE OR REPLACE FUNCTION calculate_changed_points()
RETURNS TABLE(peer VARCHAR, points_change BIGINT) AS
$$
    WITH checking_peer AS (
    SELECT checkingpeer, SUM(pointsamount) AS got_points
    FROM transferredpoints
    GROUP BY checkingpeer),

    checked_peer AS (
        SELECT checkedpeer, SUM(pointsamount) AS given_points
        FROM transferredpoints
        GROUP BY checkedpeer)

    SELECT checkedpeer AS Peer, ((COALESCE(got_points, 0)) - (COALESCE(given_points, 0))) AS pointschange
    FROM (SELECT *
          FROM checking_peer FULL JOIN checked_peer
                             ON checkingpeer = checkedpeer) AS res_table
    ORDER BY pointschange;
$$ LANGUAGE SQL;

-- SELECT * FROM calculate_changed_points();


--- 6) Посчитать изменение в количестве пир поинтов каждого пира по таблице, возвращаемой первой функцией из Part 3

DROP FUNCTION IF EXISTS calculate_changed_points_with_human_readable_tab;

CREATE OR REPLACE FUNCTION calculate_changed_points_with_human_readable_tab()
RETURNS TABLE (peer VARCHAR, pointschange INT) AS
$$
    WITH norm_t AS (
        SELECT *, CASE
                    WHEN pointsamount < 0
                    THEN
                        pointsamount * (-1)
                    ELSE
                        pointsamount
                    END AS new_points
        FROM get_transferred_points_human_readable()
    ),
    checking_peer AS (
    SELECT peer1, SUM(new_points) AS got_points
    FROM norm_t
    GROUP BY peer1
    ),
    checked_peer AS (
        SELECT peer2, SUM(new_points) AS given_points
        FROM norm_t
        GROUP BY peer2)

    SELECT COALESCE (peer1, peer2) AS Peer, ((COALESCE(got_points, 0)) - (COALESCE(given_points, 0))) AS pointschange
    FROM (SELECT *
          FROM checking_peer FULL JOIN checked_peer
                             ON peer1 = peer2) AS res_table
    ORDER BY pointschange;
$$LANGUAGE sql;

-- SELECT * FROM calculate_changed_points_with_human_readable_tab();


-- 7) Определить самое часто проверяемое задание за каждый день

DROP FUNCTION IF EXISTS get_most_frequent_check_per_day();

CREATE OR REPLACE FUNCTION get_most_frequent_check_per_day() 
RETURNS TABLE (
        "Day" DATE,
        task VARCHAR
        ) AS $$
  BEGIN
        CREATE TEMPORARY TABLE checks_count AS
        SELECT c."date", c.task, COUNT(*) as Count
          FROM checks AS c
         GROUP BY c."date", c.task;

        RETURN QUERY 
        SELECT DISTINCT c."date" AS "Day", c.task
          FROM checks AS c          
          JOIN checks_count AS sub
            ON c."date" = sub."date" AND c.task = sub.task
         WHERE sub.Count = (
               -- выбираем максимальное количество проверок за день
               SELECT MAX(Count)
                 FROM checks_count AS sub2
                WHERE sub2."date" = sub."date"
               )
        ORDER BY c."date", c.task;

        DROP TABLE IF EXISTS checks_count;
    END;
$$ LANGUAGE plpgsql;

-- SELECT * FROM get_most_frequent_check_per_day();


-- 8) Определить длительность последней P2P проверки

DROP PROCEDURE IF EXISTS duration_last_p2p_check(OUT res_time TIME);

CREATE OR REPLACE PROCEDURE duration_last_p2p_check(OUT res_time TIME) AS
$$
    SELECT (res_t.time - p2p.time)::TIME
    FROM
        (SELECT * FROM p2p
        ORDER BY id DESC, time DESC
        LIMIT 1) AS res_t
    JOIN p2p
    ON res_t."check" = p2p.check
    WHERE p2p.state = 'Start';
$$LANGUAGE sql;

-- CALL duration_last_p2p_check(NULL);


-- 9) Найти всех пиров, выполнивших весь заданный блок задач и дату завершения последнего задания

DROP FUNCTION IF EXISTS peers_done_whole_taskblock(IN block_name VARCHAR);

CREATE OR REPLACE FUNCTION peers_done_whole_taskblock(IN block_name VARCHAR)
RETURNS TABLE(peer VARCHAR, day DATE) AS
$$
    SELECT peer, "date" AS Day
    FROM checks JOIN p2p
    ON checks.id = p2p."check"
    LEFT JOIN verter
    ON checks.id = verter."check"
    WHERE p2p.state = 'Success' AND (verter.state = 'Success' OR verter.state IS NULL)
        AND task = (SELECT MAX(title) FROM tasks
                                        WHERE title ~ ('^' || $1 ||'[0-9]+_{1}'))
    ORDER BY Day DESC;
$$LANGUAGE SQL;

-- SELECT * FROM peers_done_whole_taskblock('C');


-- 10) Определить, к какому пиру стоит идти на проверку каждому обучающемуся

DROP FUNCTION IF EXISTS get_most_recommended_peer();

CREATE OR REPLACE FUNCTION get_most_recommended_peer() 
RETURNS TABLE (
        peer VARCHAR,
        recommendedpeer VARCHAR
        ) AS $$
  BEGIN
        RETURN QUERY 
          WITH all_friends AS (  -- выбираем всех друзей в двустороннем порядке
               SELECT peer1, peer2
                 FROM friends

                UNION

               SELECT peer2, peer1
                 FROM friends

                ORDER BY peer1, peer2
               ),

               sub_rec AS (  -- находим, сколько человек рекомендовало пира
               SELECT peer1, r.recommendedpeer, COUNT(*) AS Count
                 FROM recommendations AS r

                 JOIN all_friends
                   ON r.peer = peer2

                GROUP BY peer1, r.recommendedpeer
               HAVING peer1 != r.recommendedpeer
               )

        SELECT sr1.peer1 AS peer, sr1.recommendedpeer
          FROM sub_rec AS sr1
         WHERE sr1.Count = (  -- выбираем пира с максимальным количеством рекомендаций
               SELECT MAX(Count)
                 FROM sub_rec AS sr2
                WHERE sr1.peer1 = sr2.peer1
               );
    END;
$$ LANGUAGE plpgsql;

-- SELECT * FROM get_most_recommended_peer();


-- 11) Определить процент пиров, которые:
-- Приступили только к блоку 1 
-- Приступили только к блоку 2 
-- Приступили к обоим 
-- Не приступили ни к одному

DROP PROCEDURE IF EXISTS percent_of_peers;

CREATE OR REPLACE PROCEDURE percent_of_peers(IN name_block1 VARCHAR, IN name_block2 VARCHAR,
    OUT StartedBlock1 INT, OUT StartedBlock2 INT, OUT StartedBothBlocks INT, OUT DidntStartAnyBlock INT) AS
$$
    DECLARE
        count_all_peers INT;
        count_block1 INT;
        count_block2 INT;
        count_both_block INT;
        count_didnt_both_block INT;
    BEGIN
        count_all_peers := (SELECT COUNT(nickname) FROM peers);

        count_block1 := (SELECT COUNT(*)
                        FROM
                            (SELECT peer
                            FROM checks
                            WHERE task ~ ('^' || StartedBlock1 || '[0-9]+')
                            GROUP BY peer) AS count_b1);
        StartedBlock1 := count_block1 * 100 / count_all_peers;

        count_block2 := (SELECT COUNT(*)
                        FROM
                            (SELECT peer
                            FROM checks
                            WHERE task ~ ('^' || StartedBlock2 || '[0-9]+')
                            GROUP BY peer) AS count_b2);
        StartedBlock2 := count_block2 * 100 / count_all_peers;

        count_both_block := (SELECT count(*)
                            FROM
                                ((SELECT DISTINCT peer
                                FROM checks
                                WHERE task ~ ('^' || $1 || '[0-9]+'))
                                INTERSECT
                                (SELECT DISTINCT peer
                                FROM checks
                                WHERE task ~ ('^' || $2 || '[0-9]+'))) AS both_block);
        StartedBothBlocks := count_both_block * 100 / count_all_peers;

        count_didnt_both_block := (SELECT COUNT(*)
                                    FROM
                                    ((SELECT nickname FROM peers
                                    WHERE nickname NOT IN (SELECT peer FROM checks WHERE task ~ ('^' || $1 || '[0-9]+')))
                                    INTERSECT
                                    (SELECT nickname FROM peers
                                    WHERE nickname NOT IN (SELECT peer FROM checks WHERE task ~ ('^' || $2 || '[0-9]+')))) AS didnt_any_block);
        DidntStartAnyBlock := count_didnt_both_block * 100 / count_all_peers;
    END;
$$LANGUAGE plpgsql;

-- CALL percent_of_peers('SQL', 'D', NULL, NUll, NULL, NULL);


-- 12) Определить N пиров с наибольшим числом друзей

DROP FUNCTION IF EXISTS peers_with_most_friends(N INT);

CREATE OR REPLACE FUNCTION peers_with_most_friends (IN N INT)
RETURNS TABLE(Peer VARCHAR, FriendsCount INT) AS $$
    SELECT nickname AS peer, COALESCE(friendscount, 0) AS friendscount
    FROM peers LEFT JOIN (SELECT peer1, COUNT(peer2) AS friendscount
                          FROM friends
                          GROUP BY peer1
                          ORDER BY FriendsCount DESC) AS f
               ON peers.nickname = f.peer1
    ORDER BY FriendsCount DESC
    LIMIT $1;
$$LANGUAGE SQL;

-- SELECT * FROM peers_with_most_friends(N := 3);


-- 13) Определить процент пиров, которые когда-либо успешно проходили проверку в свой день рождения

DROP PROCEDURE IF EXISTS get_percent_of_birthday_checked_peers;

CREATE OR REPLACE PROCEDURE get_percent_of_birthday_checked_peers(
        OUT successfulchecks   INTEGER,
        OUT unsuccessfulchecks INTEGER
        ) AS $$
DECLARE
        all_peers INTEGER;
  BEGIN
        -- подсчитываем всех пиров
        SELECT COUNT(*)
          INTO all_peers
          FROM peers;

        -- находим ники пиров, у которых когда-либо была успешная проверка на днюху
        CREATE TEMPORARY TABLE bd_success_checked_peers AS
        SELECT DISTINCT nickname
          FROM peers
          JOIN checks
               ON nickname = peer
               AND  EXTRACT(MONTH FROM birthday) = EXTRACT(MONTH FROM "date")
               AND EXTRACT(DAY FROM birthday) = EXTRACT(DAY FROM "date")
          JOIN p2p
               ON checks.id = p2p."check"
         WHERE p2p.state = 'Success';

        -- находим ники пиров, у которых когда-либо была зафейлена проверка на днюху
        CREATE TEMPORARY TABLE bd_failure_checked_peers AS
        SELECT DISTINCT nickname
          FROM peers
          JOIN checks
               ON nickname = peer
               AND  EXTRACT(MONTH FROM birthday) = EXTRACT(MONTH FROM "date")
               AND EXTRACT(DAY FROM birthday) = EXTRACT(DAY FROM "date")
          JOIN p2p
               ON checks.id = p2p."check"
         WHERE p2p.state = 'Failure';

        -- процент пиров, сдавших проект на днюху
        SELECT ((SELECT COUNT(*)
               FROM bd_success_checked_peers) 
               * 100 / all_peers)
               INTO successfulchecks;
          
        -- процент пиров, зафейливших проверку на днюху
        SELECT ((SELECT COUNT(*)
          FROM bd_failure_checked_peers
          ) * 100 / all_peers)
          INTO unsuccessfulchecks;

        DROP TABLE IF EXISTS bd_success_checked_peers;
        DROP TABLE IF EXISTS bd_failure_checked_peers;
    END;
$$ LANGUAGE plpgsql;

-- CALL get_percent_of_birthday_checked_peers(successfulchecks := 0, unsuccessfulchecks := 0);


-- 14) Определить кол-во XP, полученное в сумме каждым пиром

DROP FUNCTION IF EXISTS get_total_xp();

CREATE OR REPLACE FUNCTION get_total_xp()
RETURNS TABLE (
        "Peer" VARCHAR,
        "XP" INT
        ) AS $$
 BEGIN
       RETURN QUERY
         WITH selected_xp AS (
              SELECT checks.peer, task, MAX(xpamount) AS xp
                FROM checks
                JOIN xp
                  ON "check" = checks.id
               GROUP BY checks.peer, task
              )
      SELECT selected_xp.peer, SUM(xp)::INT AS xp
        FROM selected_xp
       GROUP BY selected_xp.peer;

    END;
$$ LANGUAGE plpgsql;

-- SELECT * FROM get_total_xp();


-- 15) Определить всех пиров, которые сдали заданные задания 1 и 2, но не сдали задание 3

DROP FUNCTION IF EXISTS peers_did_given_task;

CREATE OR REPLACE FUNCTION peers_did_given_task(IN task1 VARCHAR, IN task2 VARCHAR, IN task3 VARCHAR)
RETURNS SETOF VARCHAR AS $$
    WITH success_task1 AS (
    SELECT peer
    FROM ((SELECT peer, "check", state AS p2p_state
           FROM checks JOIN p2p
                        ON checks.id = p2p."check"
           WHERE task ~ ('^' || $1 || '$') AND state = 'Success') AS p2p_success
          LEFT JOIN
           (SELECT "check", state AS vert_state FROM verter) AS verter_success
                ON p2p_success."check" = verter_success."check") AS res_tab
    WHERE res_tab.vert_state = 'Success' OR res_tab.vert_state IS NULL
    ),
    success_task2 AS (
        SELECT peer
        FROM ((SELECT peer, "check", state AS p2p_state
               FROM checks JOIN p2p
                            ON checks.id = p2p."check"
               WHERE task ~ ('^' || $2 || '$') AND state = 'Success') AS p2p_success
              LEFT JOIN
               (SELECT "check", state AS vert_state FROM verter) AS verter_success
                    ON p2p_success."check" = verter_success."check") AS res_tab
        WHERE res_tab.vert_state = 'Success' OR res_tab.vert_state IS NULL
    ),
    fail_task3 AS (
        (SELECT peer
        FROM checks JOIN p2p
                    ON checks.id = p2p."check"
        WHERE task ~ ('^' || $3 || '$') AND state = 'Failure')
        UNION
        (SELECT peer
         FROM checks JOIN verter
                        ON checks.id = verter."check"
        WHERE task ~ ('^' || $3 || '$') AND state = 'Failure')
    ),
    not_pass_task AS (
        SELECT nickname FROM peers
        WHERE NOT EXISTS (SELECT peer FROM checks WHERE peer = nickname AND task ~ ('^' || $3 || '$'))
    )

    SELECT * FROM success_task1
    INTERSECT
    SELECT * FROM success_task2
    INTERSECT
    (SELECT * FROM fail_task3
    UNION
    SELECT * FROM not_pass_task);

$$ LANGUAGE sql;

-- SELECT * FROM peers_did_given_task('C6_s21_matrix', 'C5_s21_decimal', 'C2_SimpleBashUtils');


-- 16) Используя рекурсивное обобщенное табличное выражение, для каждой задачи вывести кол-во предшествующих ей задач

-- вспомогательная функция: рекурсивно подсчитывает количество предшествующих задач
-- для задания, переданного в качестве параметра

DROP FUNCTION IF EXISTS count_previous_tasks;
DROP FUNCTION IF EXISTS get_counted_previous_tasks_for_all();

CREATE OR REPLACE FUNCTION count_previous_tasks(
        IN task VARCHAR
        )
RETURNS INTEGER AS $$
DECLARE
        result INTEGER;
  BEGIN
        WITH RECURSIVE r AS (
               -- lvl - это уровень вложенности (количество ParentTask'ов)
        SELECT t1.title, t1.parenttask, 0 AS lvl
          FROM tasks AS t1
         WHERE t1.title = task

         UNION

        SELECT t2.title, t2.parenttask, r.lvl + 1 AS lvl
          FROM tasks AS t2
          JOIN r
               ON t2.title = r.parenttask
         WHERE r.parenttask IS NOT NULL
        )

        SELECT MAX(lvl) INTO result FROM r;

        RETURN result;
    END;
$$ LANGUAGE plpgsql;

-- основная функция
CREATE OR REPLACE FUNCTION get_counted_previous_tasks_for_all()
RETURNS TABLE (
        task VARCHAR,
        prevcount INT
        ) AS $$
  BEGIN
        RETURN QUERY
        SELECT title,
               count_previous_tasks(title) AS prevcount
        FROM tasks;
     END;
$$ LANGUAGE plpgsql;

-- SELECT * FROM get_counted_previous_tasks_for_all();



-- 17) Найти "удачные" для проверок дни. День считается "удачным", если в нем есть хотя бы N идущих подряд успешных проверки

DROP FUNCTION IF EXISTS lucky_days;

CREATE OR REPLACE FUNCTION lucky_days(IN count INT)
RETURNS SETOF DATE AS
$$
    WITH res_t AS (
        SELECT checks.id AS checks_id, p2p.id AS p2p_id, date, p2p.time AS time,
           p2p.state AS p2p_state, verter.state AS vert_state, (xpamount * 100 / maxxp) AS percent_xp
    FROM checks
            JOIN p2p
                 ON checks.id = p2p."check"
            LEFT JOIN verter
                 ON checks.id = verter."check"
            LEFT JOIN xp
                      ON p2p."check" = xp."check"
            JOIN tasks
                 ON task = title
    WHERE p2p.state != 'Start' AND (verter.state = 'Success' OR verter.state = 'Failure' OR verter.state IS NULL)
    ORDER BY date, p2p.time
    ),
    row1_tab AS (
        SELECT ROW_NUMBER() OVER (PARTITION BY (date) ORDER BY time) AS row_num, *
        FROM res_t
    ),
    row2_tab AS (
        SELECT ROW_NUMBER() OVER (PARTITION BY (date) ORDER BY time) AS row2_num, *
        FROM row1_tab
        WHERE p2p_state = 'Success'
                AND (vert_state = 'Success' OR vert_state IS NULL)
                AND percent_xp >= 80
    )

    SELECT date
    FROM row2_tab
    WHERE row2_num - row_num = 0
    GROUP BY date
    HAVING count(*) >= $1;

$$LANGUAGE SQL;

-- SELECT * FROM lucky_days(2);


--18) Определить пира с наибольшим числом выполненных заданий

DROP PROCEDURE IF EXISTS peer_with_the_most_tasks;

CREATE OR REPLACE PROCEDURE peer_with_the_most_tasks(OUT "Peer" VARCHAR, OUT "XP" BIGINT) AS
$$
    SELECT peer, count(task) AS xp
    FROM checks JOIN p2p
                ON checks.id = p2p."check"
                LEFT JOIN verter
                ON checks.id = verter."check"
    WHERE p2p.state = 'Success' AND (verter.state = 'Success' OR verter.state IS NULL)
    GROUP BY peer
    ORDER BY xp DESC
    LIMIT 1;
$$ LANGUAGE sql;

-- CALL peer_with_the_most_tasks(NULL, NULL);


-- 19) Определить пира с наибольшим количеством XP

DROP PROCEDURE IF EXISTS get_peer_with_max_xp;

CREATE OR REPLACE PROCEDURE get_peer_with_max_xp(
       OUT "Peer" VARCHAR,
       OUT "XP"   INTEGER
) AS $$
 BEGIN
       SELECT MAX(get_total_xp."XP")
         FROM get_total_xp() 
         INTO "XP";

       SELECT get_total_xp."Peer"
         FROM get_total_xp()
         INTO "Peer"
        WHERE get_total_xp."XP" = "get_peer_with_max_xp"."XP";
    END;
$$ LANGUAGE plpgsql;

-- CALL get_peer_with_max_xp("Peer" := '', "XP" := 0);


-- 20) Определить пира, который провел сегодня в кампусе больше всего времени

DROP PROCEDURE IF EXISTS get_peer_with_most_time_today;

CREATE OR REPLACE PROCEDURE get_peer_with_most_time_today(
       OUT "Peer" VARCHAR
) AS $$
 BEGIN
         WITH today_total_time AS (
              SELECT tt1.peer,
                     ( -- находим сумму сегодняшнего времени для State 2
                     (SELECT SUM(time)
                       FROM TimeTracking AS tt2
                      WHERE tt2.peer = tt1.peer
                            AND "date" = CURRENT_DATE
                            AND state = 2)
                       -- и вычитаем из неё сумму сегодняшнего времени для State 1
                      -
                     (SELECT SUM(time)
                       FROM TimeTracking AS tt3
                      WHERE tt3.peer = tt1.peer
                            AND "date" = CURRENT_DATE
                            AND State = 1)
                     ) AS total_time
                FROM TimeTracking AS tt1
               WHERE "date" = CURRENT_DATE AND State = 2
               GROUP BY tt1.peer
               ORDER BY total_time DESC
              )
       SELECT today_total_time.peer
         FROM today_total_time
        LIMIT 1
         INTO "Peer";
   END;
$$
LANGUAGE plpgsql;

-- CALL get_peer_with_most_time_today("Peer" := '');


-- 21) Определить пиров, приходивших раньше заданного времени не менее N раз за всё время

DROP FUNCTION IF EXISTS time_coming;

CREATE OR REPLACE FUNCTION time_coming(IN time_n TIME, IN N bigint) RETURNS SETOF VARCHAR AS
$$
    SELECT peer
    FROM timetracking
    WHERE state = 1 AND time < $1
    GROUP BY peer
    HAVING count(time) >= $2;
$$LANGUAGE sql;

-- SELECT * FROM time_coming('17:30:00', 3);


-- 22) Определить пиров, выходивших за последние N дней из кампуса больше M раз

DROP FUNCTION IF EXISTS get_peers_which_exit;

CREATE OR REPLACE FUNCTION get_peers_which_exit(
        IN n INTEGER,
        IN m INTEGER
        )
RETURNS TABLE (
        nickname VARCHAR
        ) AS $$
 BEGIN
       RETURN QUERY
         WITH sub AS
              ( -- находим количество выходов в подходящие даты
              SELECT DISTINCT peer, COUNT(state) AS exits
                FROM timetracking
               GROUP BY peer, state, "date"
              HAVING state = 2
                     AND (NOW() - "date" <=  (n || ' day')::interval)
              )
       -- выбираем по сумме выходов в эти даты
       SELECT peer
         FROM sub
        GROUP BY peer
       HAVING SUM(exits) > m;       
    END;
$$ LANGUAGE plpgsql;

-- SELECT * FROM get_peers_which_exit(10, 1);


--23) Определить пира, который пришел сегодня последним

DROP PROCEDURE IF EXISTS last_come_peer_today;

CREATE OR REPLACE PROCEDURE last_come_peer_today(OUT nickname VARCHAR) AS
$$
    SELECT peer FROM timetracking
    WHERE date = current_date AND state = 1
    ORDER BY time DESC
    LIMIT 1;
$$LANGUAGE sql;

-- CALL last_come_peer_today(NULL);


-- 24) Определить пиров, которые выходили вчера из кампуса больше чем на N минут

DROP FUNCTION IF EXISTS peer_left_more_than_minutes;

CREATE OR REPLACE FUNCTION peer_left_more_than_minutes(IN time_absence TIME) RETURNS SETOF VARCHAR AS
$$
DECLARE
        N integer := 0;
        f_id INTEGER := 0;
        time_res TIME := '00:00:00';
        peer_name VARCHAR;
  BEGIN
         FOR N, f_id, peer_name IN
             SELECT COUNT(peer), MIN(id), peer FROM timetracking
             WHERE date = (current_date - 1)
             GROUP BY peer
        LOOP
             N := (N - 2) / 2;
             time_res := '00:00:00';
        
        WHILE N > 0
         LOOP
              time_res := time_res::TIME + (
               (SELECT time
                  FROM
                       (SELECT id, peer, time, state, row_number()
                          OVER (partition by (peer) ORDER BY peer, time)
                          FROM timetracking
                         WHERE date = (current_date - 1)
                       ) AS res_tab
                 WHERE id = f_id + 2 * N)::TIME 
                -
               (SELECT time
                  FROM (SELECT id, peer, time, state, row_number()
                          OVER (partition by (peer) ORDER BY peer, time)
                          FROM timetracking
                         WHERE date = (current_date - 1)
                       ) AS res2_tab
                 WHERE id = f_id + (2 * N - 1))::INTERVAL
               )::INTERVAL;
               N := N - 1;
           END LOOP;

            IF time_res > time_absence
          THEN
               RETURN NEXT peer_name;
           END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- SELECT * FROM peer_left_more_than_minutes('00:10:00');


-- 25) Определить для каждого месяца процент ранних входов

DROP FUNCTION IF EXISTS get_early_entries_at_birthday_month();

CREATE OR REPLACE FUNCTION get_early_entries_at_birthday_month()
RETURNS TABLE (
        month VARCHAR,
        earlyentries INT
        ) AS $$
 BEGIN
       CREATE TEMPORARY TABLE months (
              num INT,
              title VARCHAR
       );

       -- временная таблица для правильной сортировки по месяцам
       INSERT INTO months
       VALUES (1, 'January'),
              (2, 'February'),
              (3, 'March'),
              (4, 'April'),
              (5, 'May'),
              (6, 'June'),
              (7, 'July'),
              (8, 'August'),
              (9, 'September'),
              (10, 'October'),
              (11, 'November'),
              (12, 'December');

       RETURN QUERY
         WITH all_entries AS
              ( -- выбираем данные о пирах, которые приходили в кампус в месяц своего рождения
              SELECT peer, EXTRACT(MONTH FROM "date") AS month, "date", birthday, "time"
                FROM timetracking
                JOIN peers
                     ON peer = nickname
               GROUP BY peer, state, "date", birthday, "time"
              HAVING state = 1
                     AND EXTRACT(MONTH FROM "date") = EXTRACT(MONTH FROM birthday)
              ),
              early_entries AS
              ( -- выбираем данные о пирах, которые приходили в кампус в месяц своего рождения до 12:00
              SELECT peer, EXTRACT(MONTH FROM "date") AS month, "date", birthday, "time"
                FROM timetracking
                JOIN peers
                     ON peer = nickname
               GROUP BY peer, state, "date", birthday, "time"
              HAVING state = 1
                     AND EXTRACT(MONTH FROM "date") = EXTRACT(MONTH FROM birthday)
                     AND EXTRACT(HOUR FROM "time") < 12
              )
       SELECT sub.title, sub.earlyentries::INT
         FROM ( -- для сортировки по месяцам используем подзапрос
              SELECT DISTINCT months.num, months.title,
                     (
                     (
                      SELECT COUNT(*)
                        FROM early_entries 
                             AS ee 
                       WHERE ee.month = ae1.month 
                     ) * 100 / (
                      SELECT COUNT(*) 
                        FROM all_entries 
                             AS ae2 
                       WHERE ae2.month = ae1.month
                     )
                     ) AS earlyentries
                FROM all_entries AS ae1
                JOIN months
                     ON ae1.month = num
              ) AS sub;

       DROP TABLE IF EXISTS months;   
    END;
$$ LANGUAGE plpgsql;

-- SELECT * FROM get_early_entries_at_birthday_month();
