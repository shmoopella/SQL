-- CREATE DATABASE info21;

-- Создание таблиц

CREATE TYPE CHECK_STATUS AS ENUM ('Start', 'Success', 'Failure');

CREATE TABLE Peers (
    Nickname VARCHAR PRIMARY KEY,
    Birthday DATE
);

CREATE TABLE Tasks (
    Title      VARCHAR PRIMARY KEY,
    ParentTask VARCHAR,
    MaxXP      BIGINT,

    CONSTRAINT maxxp_is_positive CHECK(MaxXP >= 0)
);

CREATE TABLE Checks (
    ID     BIGINT,
    Peer   VARCHAR,
    Task   VARCHAR,
    Date   DATE,

    CONSTRAINT pk_Checks_ID   PRIMARY KEY (ID),
    CONSTRAINT fk_Checks_Peer FOREIGN KEY (Peer) REFERENCES Peers(Nickname)
);

CREATE TABLE P2P (
    ID           BIGINT,
    "check"      BIGINT,
    CheckingPeer VARCHAR,
    State        CHECK_STATUS,
    Time         TIME,

    CONSTRAINT pk_P2P_ID           PRIMARY KEY (ID),
    CONSTRAINT fk_P2P_Check        FOREIGN KEY ("check")      REFERENCES Checks(ID),
    CONSTRAINT fk_P2P_CheckingPeer FOREIGN KEY (CheckingPeer) REFERENCES Peers(Nickname)
);

CREATE TABLE Verter (
    ID      BIGINT,
    "check" BIGINT,
    State   CHECK_STATUS,
    Time    TIME,

    CONSTRAINT pk_Verter_ID    PRIMARY KEY (ID),
    CONSTRAINT fk_Verter_Check FOREIGN KEY ("check") REFERENCES Checks(ID)
);

CREATE TABLE TransferredPoints (
    ID           BIGINT,
    CheckingPeer VARCHAR,
    CheckedPeer  VARCHAR,
    PointsAmount BIGINT,

    CONSTRAINT pk_TransferredPoints_ID           PRIMARY KEY (ID),
    CONSTRAINT fk_TransferredPoints_CheckingPeer FOREIGN KEY (CheckingPeer) REFERENCES Peers(Nickname),
    CONSTRAINT fk_TransferredPoints_CheckedPeer  FOREIGN KEY (CheckedPeer)  REFERENCES Peers(Nickname)
);

CREATE TABLE Friends (
    ID    BIGINT,
    Peer1 VARCHAR,
    Peer2 VARCHAR,

    CONSTRAINT pk_Friends_ID    PRIMARY KEY (ID),
    CONSTRAINT fk_Friends_Peer1 FOREIGN KEY (Peer1) REFERENCES Peers(Nickname),
    CONSTRAINT fk_Friends_Peer2 FOREIGN KEY (Peer2) REFERENCES Peers(Nickname)
);

CREATE TABLE Recommendations (
  ID              BIGINT,
  Peer            VARCHAR,
  RecommendedPeer VARCHAR,

  CONSTRAINT pk_Recommendations_ID              PRIMARY KEY (ID),
  CONSTRAINT fk_Recommendations_Peer            FOREIGN KEY (Peer)            REFERENCES Peers(Nickname),
  CONSTRAINT fk_Recommendations_RecommendedPeer FOREIGN KEY (RecommendedPeer) REFERENCES Peers(Nickname)
);

CREATE TABLE XP (
    ID       BIGINT,
    "check"    BIGINT,
    XPAmount BIGINT,

    CONSTRAINT pk_XP_ID    PRIMARY KEY (ID),
    CONSTRAINT fk_XP_Check FOREIGN KEY ("check") REFERENCES Checks(ID),

    CONSTRAINT xp_amount_is_positive CHECK(XPAmount >= 0)
);

CREATE TABLE TimeTracking (
    ID      BIGINT,
    Peer    VARCHAR,
    Date    DATE,
    Time    TIME,
    State   INT,

    CONSTRAINT pk_TimeTracking_ID   PRIMARY KEY (ID),
    CONSTRAINT fk_TimeTracking_Peer FOREIGN KEY (Peer) REFERENCES Peers(Nickname),

    CONSTRAINT state_range CHECK(State BETWEEN 1 AND 2)
);


-- Объявление процедур импорта/экспорта

-- Peers

CREATE OR REPLACE PROCEDURE peers_export (
       IN delim VARCHAR(1) DEFAULT ' '
       )
       AS $$
 BEGIN
       EXECUTE FORMAT('COPY Peers TO ''/Users/%I/csv/Peers_export.csv'' DELIMITER %L', current_user, delim);
   END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE peers_import (
       IN delim VARCHAR(1) DEFAULT ' '
       )
       AS $$
 BEGIN
       EXECUTE FORMAT('COPY Peers FROM ''/Users/%I/csv/Peers_import.csv'' DELIMITER %L', current_user, delim);
   END;
$$ LANGUAGE plpgsql;

-- Tasks

CREATE OR REPLACE PROCEDURE tasks_export (
       IN delim VARCHAR(1) DEFAULT ' '
       )
       AS $$
 BEGIN
       EXECUTE FORMAT('COPY Tasks TO ''/Users/%I/csv/Tasks_export.csv'' DELIMITER %L', current_user, delim);
   END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE tasks_import (
       IN delim VARCHAR(1) DEFAULT ' '
       )
       AS $$
 BEGIN
       EXECUTE FORMAT('COPY Tasks FROM ''/Users/%I/csv/Tasks_import.csv'' DELIMITER %L', current_user, delim);
   END;
$$ LANGUAGE plpgsql;

-- Checks

CREATE OR REPLACE PROCEDURE checks_export (
       IN delim VARCHAR(1) DEFAULT ' '
       )
       AS $$
 BEGIN
       EXECUTE FORMAT('COPY Checks TO ''/Users/%I/csv/Checks_export.csv'' DELIMITER %L', current_user, delim);
   END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE checks_import (
       IN delim VARCHAR(1) DEFAULT ' '
       )
       AS $$
 BEGIN
       EXECUTE FORMAT('COPY Checks FROM ''/Users/%I/csv/Checks_import.csv'' DELIMITER %L', current_user, delim);
   END;
$$ LANGUAGE plpgsql;

-- P2P

CREATE OR REPLACE PROCEDURE p2p_export (
       IN delim VARCHAR(1) DEFAULT ' '
       )
       AS $$
 BEGIN
       EXECUTE FORMAT('COPY P2P TO ''/Users/%I/csv/P2P_export.csv'' DELIMITER %L', current_user, delim);
   END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE p2p_import (
       IN delim VARCHAR(1) DEFAULT ' '
       )
       AS $$
 BEGIN
       EXECUTE FORMAT('COPY P2P FROM ''/Users/%I/csv/P2P_import.csv'' DELIMITER %L', current_user, delim);
   END;
$$ LANGUAGE plpgsql;

-- Verter

CREATE OR REPLACE PROCEDURE verter_export (
       IN delim VARCHAR(1) DEFAULT ' '
       )
       AS $$
 BEGIN
       EXECUTE FORMAT('COPY Verter TO ''/Users/%I/csv/Verter_export.csv'' DELIMITER %L', current_user, delim);
   END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE verter_import (
       IN delim VARCHAR(1) DEFAULT ' '
       )
       AS $$
 BEGIN
       EXECUTE FORMAT('COPY Verter FROM ''/Users/%I/csv/Verter_import.csv'' DELIMITER %L', current_user, delim);
   END;
$$ LANGUAGE plpgsql;

-- TransferredPoints

CREATE OR REPLACE PROCEDURE transferredpoints_export (
       IN delim VARCHAR(1) DEFAULT ' '
       )
       AS $$
 BEGIN
       EXECUTE FORMAT('COPY TransferredPoints TO ''/Users/%I/csv/TransferredPoints_export.csv'' DELIMITER %L', current_user, delim);
   END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE transferredpoints_import (
       IN delim VARCHAR(1) DEFAULT ' '
       )
       AS $$
 BEGIN
       EXECUTE FORMAT('COPY TransferredPoints FROM ''/Users/%I/csv/TransferredPoints_import.csv'' DELIMITER %L', current_user, delim);
   END;
$$ LANGUAGE plpgsql;

-- Friends

CREATE OR REPLACE PROCEDURE friends_export (
       IN delim VARCHAR(1) DEFAULT ' '
       )
       AS $$
 BEGIN
       EXECUTE FORMAT('COPY Friends TO ''/Users/%I/csv/Friends_export.csv'' DELIMITER %L', current_user, delim);
   END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE friends_import (
       IN delim VARCHAR(1) DEFAULT ' '
       )
       AS $$
 BEGIN
       EXECUTE FORMAT('COPY Friends FROM ''/Users/%I/csv/Friends_import.csv'' DELIMITER %L', current_user, delim);
   END;
$$ LANGUAGE plpgsql;

-- Recommendations

CREATE OR REPLACE PROCEDURE recommendations_export (
       IN delim VARCHAR(1) DEFAULT ' '
       )
       AS $$
 BEGIN
       EXECUTE FORMAT('COPY Recommendations TO ''/Users/%I/csv/Recommendations_export.csv'' DELIMITER %L', current_user, delim);
   END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE recommendations_import (
       IN delim VARCHAR(1) DEFAULT ' '
       )
       AS $$
 BEGIN
       EXECUTE FORMAT('COPY Recommendations FROM ''/Users/%I/csv/Recommendations_import.csv'' DELIMITER %L', current_user, delim);
   END;
$$ LANGUAGE plpgsql;

-- XP

CREATE OR REPLACE PROCEDURE xp_export (
       IN delim VARCHAR(1) DEFAULT ' '
       )
       AS $$
 BEGIN
       EXECUTE FORMAT('COPY XP TO ''/Users/%I/csv/XP_export.csv'' DELIMITER %L', current_user, delim);
   END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE xp_import (
       IN delim VARCHAR(1) DEFAULT ' '
       )
       AS $$
 BEGIN
       EXECUTE FORMAT('COPY XP FROM ''/Users/%I/csv/XP_import.csv'' DELIMITER %L', current_user, delim);
   END;
$$ LANGUAGE plpgsql;

-- TimeTracking

CREATE OR REPLACE PROCEDURE timetracking_export (
       IN delim VARCHAR(1) DEFAULT ' '
       )
       AS $$
 BEGIN
       EXECUTE FORMAT('COPY TimeTracking TO ''/Users/%I/csv/TimeTracking_export.csv'' DELIMITER %L', current_user, delim);
   END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE timetracking_import (
       IN delim VARCHAR(1) DEFAULT ' '
       )
       AS $$
 BEGIN
       EXECUTE FORMAT('COPY TimeTracking FROM ''/Users/%I/csv/TimeTracking_import.csv'' DELIMITER %L', current_user, delim);
   END;
$$ LANGUAGE plpgsql;



-- Наполнение данными

INSERT INTO Peers (Nickname, Birthday)
VALUES ('kurdtko', '1967-02-20'),
       ('iflet', '1964-09-10'),
       ('kigordo', '1953-04-28'),
       ('msandma', '1952-09-24'),
       ('begibb', '1965-01-04'),
       ('bomarle', '1945-02-06');


INSERT INTO Tasks (Title, ParentTask, MaxXP)
VALUES ('C2_SimpleBashUtils', NULL, 350),
       ('C3_s21_string+', 'C2_SimpleBashUtils', 750),
       ('C4_s21_math', 'C2_SimpleBashUtils', 300),
       ('C5_s21_decimal', 'C3_s21_string+', 350),
       ('C6_s21_matrix', 'C5_s21_decimal', 200),
       ('C7_SmartCalc_v1.0', 'C6_s21_matrix', 500),
       ('C8_3DViewer_v1.0', 'C7_SmartCalc_v1.0', 750),
       ('DO1_Linux', 'C3_s21_string+', 300),
       ('DO2_Linux_Network', 'DO1_Linux', 250),
       ('DO3_LinuxMonitoring_v1.0', 'DO2_Linux_Network', 350),
       ('DO4_LinuxMonitoring_v2.0', 'DO3_LinuxMonitoring_v1.0', 350),
       ('DO5_SimpleDocker', 'DO3_LinuxMonitoring_v1.0', 300),
       ('DO6_CICD', 'DO5_SimpleDocker', 300),
       ('DOE-T', 'DO6_CICD', 200),
       ('CPP1_s21_matrix+', 'C8_3DViewer_v1.0', 300),
       ('CPP2_s21_containers', 'CPP1_s21_matrix+', 350),
       ('CPP3_SmartCalc_v2.0', 'CPP2_s21_containers', 600),
       ('CPP4_3DViewer_v2.0', 'CPP3_SmartCalc_v2.0', 750);


INSERT INTO TimeTracking (ID, Peer, Date, Time, State)
VALUES (1, 'iflet', '2023-02-10', '15:50:00', 1),
       (2, 'iflet', '2023-02-10', '23:50:05', 2),
       (3, 'begibb', '2023-02-11', '12:45:56', 1),
       (4, 'begibb', '2023-02-11', '16:51:16', 2),
       (5, 'msandma', '2023-02-13', '08:03:11', 1),
       (6, 'msandma', '2023-02-13', '18:10:09', 2),
       (7, 'kigordo', '2023-03-10', '08:50:00', 1),
       (8, 'kigordo', '2023-03-10', '23:50:05', 2),
       (9, 'kigordo', '2023-03-11', '12:01:56', 1),
       (10, 'kigordo', '2023-03-11', '20:51:16', 2),
       (11, 'kigordo', '2023-03-11', '09:03:11', 1),
       (12, 'kigordo', '2023-03-11', '19:10:09', 2),
       (13, 'kurdtko', '2023-03-11', '11:03:11', 1),
       (14, 'kurdtko', '2023-03-11', '13:10:09', 2),
       (15, 'kurdtko', '2023-03-11', '14:03:11', 1),
       (16, 'kurdtko', '2023-03-11', '16:10:09', 2),
       (17, 'iflet', '2023-03-11', '12:12:11', 1),
       (18, 'iflet', '2023-03-11', '20:10:00', 2),
       (19, 'begibb', '2023-01-01', '08:50:00', 1),
       (20, 'begibb', '2023-01-01', '23:50:05', 2),
       (21, 'begibb', '2023-01-02', '12:01:56', 1),
       (22, 'begibb', '2023-01-02', '20:51:16', 2),
       (23, 'begibb', '2023-01-03', '09:03:11', 1),
       (24, 'begibb', '2023-01-03', '19:10:09', 2),
       (25, 'kurdtko', '2023-02-14', '11:03:11', 1),
       (26, 'kurdtko', '2023-02-14', '13:10:09', 2),
       (27, 'kurdtko', '2023-02-15', '14:03:11', 1),
       (28, 'kurdtko', '2023-02-15', '16:10:09', 2),
       (29, 'bomarle', '2023-02-16', '11:12:11', 1),
       (30, 'bomarle', '2023-02-16', '23:10:00', 2),
       (31, 'bomarle', '2023-02-17', '10:12:11', 1),
       (32, 'bomarle', '2023-02-17', '22:10:00', 2),
       (33, 'msandma', '2023-03-17', '13:15:01', 1),
       (34, 'msandma', '2023-03-17', '15:45:00', 2),
       (35, 'msandma', '2023-03-17', '17:15:01', 1),
       (36, 'msandma', '2023-03-17', '19:01:15', 2),
       (37, 'msandma', '2023-03-17', '23:29:08', 1),
       (38, 'msandma', '2023-03-17', '23:55:01', 2),
       (39, 'iflet', '2023-03-17', '10:15:01', 1),
       (40, 'iflet', '2023-03-17', '12:45:00', 2),
       (41, 'iflet', '2023-03-17', '13:02:01', 1),
       (42, 'iflet', '2023-03-17', '16:01:15', 2),
       (43, 'iflet', '2023-03-17', '19:29:08', 1),
       (44, 'iflet', '2023-03-17', '23:55:01', 2),
       (45, 'kurdtko', '2023-03-19', '07:12:03', 1),
       (46, 'kurdtko', '2023-03-19', '12:45:00', 2),
       (47, 'kurdtko', '2023-03-19', '12:49:01', 1),
       (48, 'kurdtko', '2023-03-19', '16:01:15', 2),
       (49, 'iflet', CURRENT_DATE, '03:20:00', 1),
       (50, 'iflet', CURRENT_DATE, '06:20:00', 2),
       (51, 'iflet', CURRENT_DATE, '14:00:00', 1),
       (52, 'iflet', CURRENT_DATE, '23:00:00', 2),
       (53, 'bomarle', CURRENT_DATE, '09:00:00', 1),
       (54, 'bomarle', CURRENT_DATE, '18:15:00', 2),
       (55, 'kigordo', CURRENT_DATE, '10:00:00', 1),
       (56, 'kigordo', CURRENT_DATE, '23:50:00', 2),
       (57, 'kurdtko', CURRENT_DATE - 1, '03:20:00', 1),
       (58, 'kurdtko', CURRENT_DATE - 1, '06:20:00', 2),
       (59, 'kurdtko', CURRENT_DATE - 1, '14:00:00', 1),
       (61, 'kurdtko', CURRENT_DATE - 1, '23:00:00', 2),
       (62, 'bomarle', CURRENT_DATE - 1, '07:15:00', 1),
       (63, 'bomarle', CURRENT_DATE - 1, '10:00:00', 2),
       (64, 'bomarle', CURRENT_DATE - 1, '16:00:00', 1),
       (65, 'bomarle', CURRENT_DATE - 1, '19:00:00', 2);



INSERT INTO Friends (ID, Peer1, Peer2)
VALUES (1, 'iflet', 'begibb'),
       (2, 'iflet', 'msandma'),
       (3, 'kurdtko', 'kigordo'),
       (4, 'kurdtko', 'begibb'),
       (5, 'msandma', 'kigordo');


INSERT INTO Recommendations (ID, Peer, RecommendedPeer)
VALUES (1, 'iflet', 'begibb'),
       (2, 'begibb', 'msandma'),
       (3, 'msandma', 'kigordo'),
       (4, 'begibb', 'kigordo'),
       (5, 'iflet', 'kurdtko');


INSERT INTO checks (ID, Peer, Task, "date")
VALUES (1, 'kurdtko', 'C6_s21_matrix', '2023-01-14'),
       (2, 'kigordo', 'C4_s21_math', '2023-01-19'),
       (3, 'msandma', 'C2_SimpleBashUtils', '2023-01-29'),
       (4, 'begibb', 'C2_SimpleBashUtils', '2023-01-04'),
       (5, 'begibb', 'C3_s21_string+', '2023-03-01'),
       (6, 'kurdtko', 'C4_s21_math', '2023-02-20'),
       (7, 'begibb', 'C4_s21_math', '2023-03-03'),
       (8, 'kigordo', 'C5_s21_decimal', '2023-03-03'),
       (9, 'iflet', 'C4_s21_math', '2023-03-14'),
       (10, 'begibb', 'C5_s21_decimal', '2023-03-03'),
       (11, 'begibb', 'C6_s21_matrix', '2023-03-04'),
       (12, 'begibb', 'C7_SmartCalc_v1.0', '2023-03-05'),
       (13, 'begibb', 'C8_3DViewer_v1.0', '2023-03-05'),
       (14, 'begibb', 'DO1_Linux', '2023-03-21'),
       (15, 'bomarle', 'C2_SimpleBashUtils', '2023-03-22'),
       (16, 'kurdtko', 'C7_SmartCalc_v1.0', '2023-03-24'),
       (17, 'iflet', 'C5_s21_decimal', '2023-03-24'),
       (18, 'kigordo', 'C6_s21_matrix', '2023-03-24'),
       (19, 'msandma', 'C3_s21_string+', '2023-03-24'),
       (20, 'begibb', 'DO2_Linux_Network', '2023-03-24'),
       (21, 'bomarle', 'C3_s21_string+', '2023-03-24');


INSERT INTO P2P (ID, "check", CheckingPeer, State, Time)
VALUES (1, 1, 'iflet', 'Start', '01:54:55'),
       (2, 1, 'iflet', 'Success', '02:14:59'),
       (3, 2, 'msandma', 'Start', '19:44:03'),
       (4, 2, 'msandma', 'Success', '20:03:14'),
       (5, 3, 'begibb', 'Start', '15:15:57'),
       (6, 3, 'begibb', 'Success', '15:30:50'),
       (7, 6, 'kigordo', 'Start', '17:20:01'),
       (8, 6, 'kigordo', 'Success', '17:37:01'),
       (9, 4, 'msandma', 'Start', '13:02:02'),
       (10, 4, 'msandma', 'Failure', '13:35:14'),
       (11,8, 'begibb', 'Start', '16:14:12'),
       (12,8, 'begibb', 'Success', '16:14:12'),
       (13,9, 'kurdtko', 'Start', '03:50:00'),
       (14, 9, 'kurdtko', 'Success', '03:52:34'),
       (15, 10, 'iflet', 'Start', '02:14:59'),
       (16, 10, 'iflet', 'Success', '02:24:01'),
       (17, 11, 'iflet', 'Start', '13:14:59'),
       (18, 11, 'iflet', 'Success', '13:34:02'),
       (19, 12, 'iflet', 'Start', '15:04:02'),
       (20, 12, 'iflet', 'Success', '15:34:02'),
       (21, 13, 'iflet', 'Start', '09:34:02'),
       (22, 13, 'iflet', 'Success', '09:54:22'),
       (23, 7, 'kurdtko', 'Start', '13:34:02'),
       (24, 7, 'kurdtko', 'Success', '13:39:00'),
       (25, 5, 'msandma', 'Start', '19:44:03'),
       (26, 5, 'msandma', 'Success', '19:55:01'),
       (27, 15, 'iflet', 'Start', '12:14:59'),
       (28, 15, 'iflet', 'Failure', '12:29:12'),
       (29, 14, 'kigordo', 'Start', '19:44:03'),
       (30, 14, 'kigordo', 'Success', '19:59:02'),
       (31, 16, 'iflet', 'Start', '19:59:02'),
       (32, 16, 'iflet', 'Success', '20:21:02'),
       (33, 17, 'kigordo', 'Start', '21:09:02'),
       (34, 17, 'kigordo', 'Success', '22:59:02'),
       (35, 18, 'msandma', 'Start', '21:12:02'),
       (36, 18, 'msandma', 'Success', '21:14:02'),
       (37, 19, 'kigordo', 'Start', '21:47:05'),
       (38, 19, 'kigordo', 'Success', '22:01:02'),
       (39, 20, 'kigordo', 'Start', '23:01:02'),
       (40, 20, 'kigordo', 'Success', '23:14:02'),
       (41, 21, 'kigordo', 'Start', '23:23:02'),
       (42, 21, 'kigordo', 'Success', '23:48:02');



INSERT INTO Verter (ID, "check", State, Time)
VALUES (1, 1, 'Start', '02:15:15'),
      (2, 1, 'Success', '02:16:15'),
      (3, 2, 'Start', '20:04:03'),
      (4, 2, 'Success', '20:05:14'),
      (5, 3, 'Start', '15:32:11'),
      (6, 3, 'Failure', '15:36:17'),
      (7, 6, 'Start', '18:11:11'),
      (8, 6, 'Success', '18:13:56'),
      (9, 9,'Start', '03:55:01'),
      (10, 9, 'Success', '03:56:09'),
      (11, 10, 'Start', '02:25:10'),
      (12, 10, 'Success', '02:27:15'),
      (13, 7, 'Start', '13:40:00'),
      (14, 7, 'Failure', '13:49:00'),
      (15, 4, 'Start', '18:11:11'),
      (16, 4, 'Success', '18:13:11'),
      (17, 5, 'Start', '10:11:11'),
      (18, 5, 'Success', '12:11:11'),
      (19, 15, 'Start', '12:31:09'),
      (20, 15, 'Failure', '12:33:22'),
      (21, 17, 'Start', '23:05:07'),
      (22, 17, 'Failure', '23:07:07');


INSERT INTO TransferredPoints (ID, CheckingPeer, CheckedPeer, PointsAmount)
VALUES (1, 'iflet', 'kurdtko', 1),
       (2, 'msandma', 'kigordo', 3),
       (3, 'begibb', 'msandma', 2),
       (4, 'iflet', 'begibb', 2),
       (5, 'kigordo', 'msandma', 4),
       (6, 'begibb', 'iflet', 1);

INSERT INTO Xp (ID, "check", XPAmount)
VALUES (1, 1, 200),
       (2, 2, 300),
       (3, 6, 300),
       (4, 5, 320),
       (5, 10, 350),
       (6, 11, 200),
       (7, 12, 500),
       (8, 13, 750),
       (9, 9, 300),
       (10, 8, 350),
       (11, 14, 300),
       (12, 16, 500),
       (13, 18, 200),
       (14, 19, 750),
       (15, 20, 250),
       (16, 21, 750);