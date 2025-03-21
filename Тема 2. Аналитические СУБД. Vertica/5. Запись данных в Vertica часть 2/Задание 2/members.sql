CREATE TABLE members
(
    id int NOT NULL,
    age int,
    gender char,
    email varchar(50),
    CONSTRAINT C_PRIMARY PRIMARY KEY (id) DISABLED
);
COPY STV2025021827.members ( id, age,gender,email  ENFORCELENGTH )
FROM LOCAL 'C:\Users\Ethan\Gold_recovery_Project\project_de\sprint6\s6-lessons\Тема 2. Аналитические СУБД. Vertica\5. Запись данных в Vertica часть 2\Задание 2\members.csv'
DELIMITER ';'
REJECTED DATA AS TABLE STV2025021827.members_rej
;