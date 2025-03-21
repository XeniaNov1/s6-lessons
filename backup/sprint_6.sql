CREATE TABLE STV2025021827.BAD_IDEA (
    i int primary key,
    v varchar(16)
);
ALTER TABLE STV2025021827.BAD_IDEA SET MERGEOUT 0;
ALTER TABLE STV2025021827.BAD_IDEA SET MERGEOUT 1;
COPY STV2025021827.BAD_IDEA (i, v)
FROM LOCAL 'C:\Users\Ethan\Gold_recovery_Project\project_de\sprint6\s6-lessons\test_dataset.csv'
DELIMITER '|';

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
drop table STV2025021827.members_rej;
delete from members_rej;
delete from members;

CREATE TABLE members_v2
(
    id varchar(2000) NOT NULL,
        age varchar(2000),
    gender varchar(2000),
    email varchar(2000),
    CONSTRAINT C_PRIMARY PRIMARY KEY (id) DISABLED
);
insert into STV2025021827.members_v2
select * from members;

select 
        anchor_table_schema,
        anchor_table_name,
        sum(used_bytes) / (1024/1024/1024/1024) AS TABLE_SIZE_GB
from
        v_monitor.projection_storage
where 
        anchor_table_name in ('members', 'members_v2')
group by
        anchor_table_schema,
        anchor_table_name
order by 
        sum(used_bytes) desc;
 
       
       
 create table STV2025021827.orders
(
    id  varchar(2000) PRIMARY KEY,
    registration_ts timestamp(6),
    user_id varchar(2000),
    is_confirmed int
)
ORDER BY id
SEGMENTED BY HASH(id) ALL NODES
;
create table STV2025021827.orders_v2
(
    id  int PRIMARY KEY,
    registration_ts timestamp(6),
    user_id int,
    is_confirmed boolean
)
ORDER BY id
SEGMENTED BY HASH(id) ALL NODES
;
drop table STV2025021827.orders_v2;

CREATE TABLE COPY_EX1 (
    i int primary key,
    v varchar(16)
);

/* 
Вставка хотя бы одной записи обязательна для нашего примера
*/
INSERT INTO COPY_EX1 VALUES (1, 'wow');

select export_objects('','COPY_EX1')



CREATE TABLE STV2025021827.COPY_EX1
(
    i int NOT NULL,
    v varchar(16),
    CONSTRAINT C_PRIMARY PRIMARY KEY (i) DISABLED
);


CREATE PROJECTION STV2025021827.COPY_EX1 /*+createtype(L)*/ 
(
 i,
 v
)
AS
 SELECT COPY_EX1.i,
        COPY_EX1.v
 FROM STV2025021827.COPY_EX1
 ORDER BY COPY_EX1.i
SEGMENTED BY hash(COPY_EX1.i) ALL NODES KSAFE 1;


SELECT MARK_DESIGN_KSAFE(1);

----------------
CREATE TABLE {ВАША СХЕМА}.COPY_EX1
(
    i int NOT NULL,
    v varchar(16),
    CONSTRAINT C_PRIMARY PRIMARY KEY (i) DISABLED
);

CREATE PROJECTION {ВАША СХЕМА}.COPY_EX1 /*+createtype(L)*/ 
(
 i,
 v
)
AS
 SELECT COPY_EX1.i,
        COPY_EX1.v
 FROM {ВАША СХЕМА}.COPY_EX1
 ORDER BY COPY_EX1.i
SEGMENTED BY hash(COPY_EX1.i) ALL NODES KSAFE 1;

SELECT MARK_DESIGN_KSAFE(1);


CREATE TABLE MY_TABLE (
    i int,
    ts timestamp(6),
    v varchar(1024),
    PRIMARY KEY (i, ts)
)
ORDER BY i, v, ts
SEGMENTED BY HASH(i, ts) ALL NODES;


--Прекод
create table STV2025021827.dialogs
(
    message_id   int PRIMARY KEY,
    message_ts   timestamp(6),
    message_from int REFERENCES members(id),
    message_to int REFERENCES members(id),
    message varchar(1000),
    message_type varchar(100)
)
SEGMENTED BY hash(message_id) all nodes;
drop table STV2025021827.dialogs;


drop table if exists STV2025021827.members;

create table STV2025021827.members
(
    id int PRIMARY KEY,
    age int,
    gender varchar(8),
    email varchar(256)
)
order by id
segmented by hash(id) all nodes;

copy STV2025021827.members (
        id, age, gender, email) 
from 
        local 'C:\Users\Ethan\Gold_recovery_Project\project_de\sprint6\s6-lessons\members.csv'
delimiter ';';


drop table if exists STV2025021827.dialogs;

create table STV2025021827.dialogs
(
    message_id   int PRIMARY KEY,
    message_ts   timestamp(6),
    message_from int REFERENCES members(id),
    message_to   int REFERENCES members(id),
    message      varchar(1000),
    message_group int
)
order by message_from, message_ts
segmented by hash(message_id) all nodes;

copy STV2025021827.dialogs (
        message_id, message_ts, message_from, message_to, message, message_group) 
from 
        local 'C:\Users\Ethan\Gold_recovery_Project\project_de\sprint6\s6-lessons\dialogs.csv'
delimiter ',';

--11 удаление и обновление данных
--1 задание
SET SESSION AUTOCOMMIT TO off;
    
DELETE FROM members WHERE age > 45;

SELECT node_name, projection_name, deleted_row_count FROM DELETE_VECTORS
    where projection_name like 'members%';

SELECT max(deleted_row_count) FROM DELETE_VECTORS
    where projection_name like 'members%';

ROLLBACK;

--2 задание

TRUNCATE TABLE members;

SELECT node_name, projection_name, deleted_row_count FROM DELETE_VECTORS
    where projection_name like 'members%';

ROLLBACK;

--3 задание
COPY members(id, age, gender, email ENFORCELENGTH )
FROM LOCAL 'C:\Users\Ethan\Gold_recovery_Project\project_de\sprint6\s6-lessons\Тема 2. Аналитические СУБД. Vertica\5. Запись данных в Vertica часть 2\Задание 2\members.csv'
DELIMITER ';'
REJECTED DATA AS TABLE members_rej;

create table members_inc
(
        id int NOT NULL,
        age int,
        gender char,
        email varchar(50),
        CONSTRAINT C_PRIMARY PRIMARY KEY (id) DISABLED
)
ORDER BY id
SEGMENTED BY HASH(id) ALL NODES
;

copy members_inc (id, age, gender, email) 
from 
    local 'C:\Users\Ethan\Gold_recovery_Project\project_de\sprint6\s6-lessons\Тема 2. Аналитические СУБД. Vertica\11. Удаление и обновление данных в Vertica\Задание 3\members_inc.csv'
delimiter ';'


MERGE INTO 
    members tgt /* имя таблицы в которой будут обновляться данные и в которую будут вставлены новые записи*/
USING
    members_inc src /*таблица или подзапрос, из которой нужно взять данные*/
ON  /* ключи MERGE */
    tgt.id = src.id 
WHEN MATCHED and (
    tgt.id = src.id
)
    THEN UPDATE SET 
                gender = src.gender, 
                email = src.email, 
                age = src.age 
WHEN NOT MATCHED
    THEN INSERT (
                id,
                age, 
                gender, 
                email
        )
    VALUES (
                src.id,
                src.age,
                src.gender,
                src.email
        )
;

--12 партиционирование таблиц
/* Прекод */
drop table if exists dialogs;

create table dialogs
(
    message_id   int PRIMARY KEY,
    message_ts   timestamp(6) NOT NULL,
    message_from int REFERENCES members(id) NOT NULL,
    message_to   int REFERENCES members(id) NOT NULL,
    message      varchar(1000),
    message_type varchar(100)
)
order by message_from, message_ts
SEGMENTED BY hash(message_id) all nodes
PARTITION BY message_ts::DATE 
;

--13 операции с партициями

--2 задание
drop table if exists dialogs;
create table dialogs
(
    message_id   int PRIMARY KEY,
    message_ts   timestamp(6) NOT NULL,
    message_from int REFERENCES members(id) NOT NULL,
    message_to   int REFERENCES members(id) NOT NULL,
    message      varchar(1000),
    message_type varchar(100)
)
order by message_from, message_ts
SEGMENTED BY hash(message_id) all nodes
PARTITION BY message_ts::date
GROUP BY calendar_hierarchy_day(message_ts::date, 3, 2)
;


--13. Операции с партициями

drop table if exists dialogs;
/* Если схема не указана, то будет 
использована схема по умолчанию = логину */

create table dialogs 
(
    message_id   int PRIMARY KEY,
    message_ts   timestamp(6) NOT NULL,
    message_from int NOT NULL,
    message_to   int NOT NULL,
    message      varchar(1000),
    message_type varchar(100)
)
order by 
        message_from, message_ts
segmented by 
        hash(message_id) all nodes
partition by 
        message_ts::date
group by 
        calendar_hierarchy_day(message_ts::date, 3, 2)
;

COPY dialogs (
    message_id,
    /* FILLER - означает, что в этот атрибут вектора загрузки
    мы читаем значение из файла. Но на выход (в таблицу) оно
    уже не поступит. Зато можно использовать message_ts_orig
    для трансформации прямо в процессе загрузки */
    message_ts_orig FILLER timestamp(6), 
    message_ts as 
        /* Поскольку данные статичны, а время идёт вперёд, 
        для демонстрации мы сдвинем 2 последних года поближе
        к реальному времени */
        add_months(
            message_ts_orig, 
            case 
                when year(message_ts_orig) >= 2020 
                then datediff('month', '2021-06-21'::timestamp, now()::timestamp) - 1
                else 0
            end
        ),
    message_from,
    message_to,
    message,
    message_type
)
/* 
    NOTE: здесь надо указать путь к файлу на вашем компьютере.
    Для UNC путей (Windows), на забываем экранировать '\' 
    с помощью двойного указания. Например 'C:\\Files\\dialogs.csv'
*/
from 
        local 'C:\Users\Ethan\Gold_recovery_Project\project_de\sprint6\s6-lessons\Тема 2. Аналитические СУБД. Vertica\13. Операции с партициями\dialogs.csv'
delimiter ','
ENCLOSED BY '"';

SELECT DROP_PARTITIONS(
    'dialogs', 
    '2004-01-01',
  '2005-12-31');
 
 
 --тема 3 разработка аналитической базы данных
 -- 3 создадим стг слой
 drop table if exists STV2025021827__STAGING.users;
 create table STV2025021827__STAGING.users (
 	id int not null,
 	chat_name varchar(200),
 	registration_dt timestamp,
 	country varchar(200),
 	age int
 )
 	order by id
 	SEGMENTED BY HASH(id) ALL NODES;
 	
 	
  drop table if exists STV2025021827__STAGING.dialogs;
 create table STV2025021827__STAGING.dialogs (
 	message_id int not null,
 	message_ts timestamp,
 	message_from int,
 	message_to int,
 	message varchar(1000),
 	message_group int
 )
 	order by message_id
 	SEGMENTED BY HASH(message_id) ALL NODES
 	PARTITION BY message_ts::date
	GROUP BY calendar_hierarchy_day(message_ts::date, 3, 2);
	
  drop table if exists STV2025021827__STAGING.groups;
 create table STV2025021827__STAGING.groups (
 	id int not null,
 	admin_id int,
 	group_name varchar(100),
 	registration_dt timestamp,
 	is_private boolean
 )
 	order by id, admin_id
 	SEGMENTED BY HASH(id) ALL NODES
 	PARTITION BY registration_dt::date
	GROUP BY calendar_hierarchy_day(registration_dt::date, 3, 2);
	

copy STV2025021827__STAGING.dialogs (
     message_id, message_ts, message_from, message_to, message, message_group) 
     from 
     local 'C:\Users\Ethan\Gold_recovery_Project\project_de\sprint6\s6-lessons\Тема 3.  Разработка аналитической базы данных\3. Создадим staging-слой\Задание 2\dialogs.csv'
     delimiter ',';
                   
copy STV2025021827__STAGING.users (
     id, chat_name, registration_dt,	country, age) 
     from 
     local 'C:\Users\Ethan\Gold_recovery_Project\project_de\sprint6\s6-lessons\Тема 3.  Разработка аналитической базы данных\3. Создадим staging-слой\Задание 2\users.csv'
     delimiter ',';
                   
copy STV2025021827__STAGING.groups (
     id, admin_id, group_name, registration_dt, is_private) 
     from 
     local 'C:\Users\Ethan\Gold_recovery_Project\project_de\sprint6\s6-lessons\Тема 3.  Разработка аналитической базы данных\3. Создадим staging-слой\Задание 2\groups.csv'
     delimiter ',';
                   
select count(id) as total, count(distinct id) as uniq, 'users' as dataset
from STV2025021827__STAGING.users
union all
select count(id) as total, count(distinct id) as uniq, 'groups' as dataset
from STV2025021827__STAGING.groups
union all
select count(message_id) as total, count(distinct message_id) as uniq, 'dialogs' as dataset
from STV2025021827__STAGING.dialogs


--разработка аналитического хранилища

--задание 1
drop table if exists STV2025021827__DWH.h_users;


create table STV2025021827__DWH.h_users
(
    hk_user_id bigint primary key,
    user_id      int,
    registration_dt datetime,
    load_dt datetime,
    load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_user_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);
;

drop table if exists STV2025021827__DWH.h_dialogs;
create table STV2025021827__DWH.h_dialogs
(
	hk_message_id bigint primary key,
	message_id int,
	message_ts timestamp,
	load_dt datetime,
    load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_message_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2)
;



drop table if exists STV2025021827__DWH.h_groups;
create table STV2025021827__DWH.h_groups 
(
	hk_group_id bigint primary key,
 	group_id int,
 	registration_dt timestamp,
	load_dt datetime,
    load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_group_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2)
;

--задание 2

INSERT INTO STV2025021827__DWH.h_users(hk_user_id, user_id,registration_dt,load_dt,load_src)
select
       hash(id) as  hk_user_id,
       id as user_id,
       registration_dt,
       now() as load_dt,
       's3' as load_src
       from STV2025021827__STAGING.users
where hash(id) not in (select hk_user_id from STV2025021827__DWH.h_users);

INSERT INTO STV2025021827__DWH.h_dialogs(hk_message_id, message_id, message_ts, load_dt, load_src)
select
       hash(message_id) as  hk_message_id,
       message_id as message_id,
       message_ts,
       now() as load_dt,
       's3' as load_src
       from STV2025021827__STAGING.dialogs
where hash(message_id) not in (select hk_message_id from STV2025021827__DWH.h_dialogs);

INSERT INTO STV2025021827__DWH.h_groups(hk_group_id, group_id, registration_dt, load_dt, load_src)
select
       hash(id) as  hk_group_id,
       id as group_id,
       registration_dt,
       now() as load_dt,
       's3' as load_src
       from STV2025021827__STAGING.groups
where hash(id) not in (select hk_group_id from STV2025021827__DWH.h_groups);


--задание 3

drop table if exists STV2025021827__DWH.l_user_message;

create table STV2025021827__DWH.l_user_message
(
	hk_l_user_message bigint primary key,
	hk_user_id bigint not null CONSTRAINT fk_l_user_message_user REFERENCES STV2025021827__DWH.h_users (hk_user_id),
	hk_message_id bigint not null CONSTRAINT fk_l_user_message_message REFERENCES STV2025021827__DWH.h_dialogs (hk_message_id),
	load_dt datetime,
	load_src varchar(20)
)

order by load_dt
SEGMENTED BY hk_user_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

drop table if exists STV2025021827__DWH.l_admins;

create table STV2025021827__DWH.l_admins
(
	hk_l_admin_id bigint primary key,
	hk_user_id bigint not null CONSTRAINT fk_l_admins_user REFERENCES STV2025021827__DWH.h_users (hk_user_id),
	hk_group_id bigint not null CONSTRAINT fk_l_admins_group REFERENCES STV2025021827__DWH.h_groups (hk_group_id),
	load_dt datetime,
	load_src varchar(20)
)

order by load_dt
SEGMENTED BY hk_l_admin_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

drop table if exists STV2025021827__DWH.l_groups_dialogs;

create table STV2025021827__DWH.l_groups_dialogs
(
	hk_l_groups_dialogs bigint primary key,
	hk_message_id bigint not null CONSTRAINT fk_l_groups_dialogs_message REFERENCES STV2025021827__DWH.h_dialogs (hk_message_id),
	hk_group_id bigint not null CONSTRAINT fk_l_groups_dialogs_groups REFERENCES STV2025021827__DWH.h_groups (hk_group_id),
	load_dt datetime,
	load_src varchar(20)
)

order by load_dt
SEGMENTED BY hk_l_groups_dialogs all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);


--задание 4

INSERT INTO STV2025021827__DWH.l_admins(hk_l_admin_id, hk_group_id,hk_user_id,load_dt,load_src)
	select
	hash(hg.hk_group_id,hu.hk_user_id),
	hg.hk_group_id,
	hu.hk_user_id,
	now() as load_dt,
	's3' as load_src
from STV2025021827__STAGING.groups as g
left join STV2025021827__DWH.h_users as hu on g.admin_id = hu.user_id
left join STV2025021827__DWH.h_groups as hg on g.id = hg.group_id
where hash(hg.hk_group_id,hu.hk_user_id) not in (select hk_l_admin_id from STV2025021827__DWH.l_admins);


INSERT INTO STV2025021827__DWH.l_user_message(hk_l_user_message, hk_user_id,hk_message_id,load_dt,load_src)
	select
	hash(hu.hk_user_id,hd.hk_message_id),
	hd.hk_message_id,
	hu.hk_user_id,
	now() as load_dt,
	's3' as load_src
from STV2025021827__STAGING.dialogs as d
left join STV2025021827__DWH.h_users as hu on d.message_from = hu.user_id
left join STV2025021827__DWH.h_dialogs as hd on d.message_id = hd.message_id
where hash(hu.hk_user_id,hd.hk_message_id) not in (select hk_l_user_message from STV2025021827__DWH.l_user_message);

INSERT INTO STV2025021827__DWH.l_groups_dialogs (hk_l_groups_dialogs, hk_message_id, hk_group_id, load_dt, load_src)
	select 
	hash(hd.hk_message_id, hg.hk_group_id),
	hd.hk_message_id,
	hg.hk_group_id,
	now() as load_dt,
	's3' as load_src
from STV2025021827__STAGING.dialogs d
left join STV2025021827__DWH.h_dialogs hd on d.message_id = hd.message_id
left join STV2025021827__DWH.h_groups hg on d.message_group = hg.group_id
where hash(hd.hk_message_id, hg.hk_group_id) not in (select hk_l_groups_dialogs from STV2025021827__DWH.l_groups_dialogs);


--задание 5

--s_admins
drop table if exists STV2025021827__DWH.s_admins;

create table STV2025021827__DWH.s_admins
(
hk_admin_id bigint not null CONSTRAINT fk_s_admins_l_admins REFERENCES STV2025021827__DWH.l_admins (hk_l_admin_id),
is_admin boolean,
admin_from datetime,
load_dt datetime,
load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_admin_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);


INSERT INTO STV2025021827__DWH.s_admins(hk_admin_id, is_admin,admin_from,load_dt,load_src)
select la.hk_l_admin_id,
True as is_admin,
hg.registration_dt,
now() as load_dt,
's3' as load_src
from STV2025021827__DWH.l_admins as la
left join STV2025021827__DWH.h_groups as hg on la.hk_group_id = hg.hk_group_id;


--s_group_name
drop table if exists STV2025021827__DWH.s_group_name;

create table STV2025021827__DWH.s_group_name
	(
	hk_group_id bigint not null CONSTRAINT fk_s_group_name_l_groups_dialogs REFERENCES STV2025021827__DWH.h_groups (hk_group_id), 
	group_name varchar(100),
	load_dt datetime,
	load_src varchar(20)
	)
order by load_dt
SEGMENTED BY hk_group_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

	
INSERT INTO STV2025021827__DWH.s_group_name(hk_group_id, group_name,load_dt,load_src)
 select distinct hg.hk_group_id,
 		g.group_name,
 		now() as load_dt,
		's3' as load_src
from STV2025021827__DWH.h_groups hg
left join STV2025021827__STAGING.groups g on hg.group_id = g.id;

--s_group_private_status
drop table if exists STV2025021827__DWH.s_group_private_status;

create table STV2025021827__DWH.s_group_private_status
	(
	hk_group_id bigint not null CONSTRAINT fk_s_group_name_l_groups_dialogs REFERENCES STV2025021827__DWH.h_groups (hk_group_id), 
	is_private boolean,
	load_dt datetime,
	load_src varchar(20)
	)
order by load_dt
SEGMENTED BY hk_group_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

INSERT INTO STV2025021827__DWH.s_group_private_status(hk_group_id, is_private,load_dt,load_src)
	select distinct hg.hk_group_id,
			g.is_private,
			now() as load_dt,
			's3' as load_src
from STV2025021827__DWH.h_groups hg
left join STV2025021827__STAGING.groups g on hg.group_id = g.id;
			
--s_dialog_info
drop table if exists STV2025021827__DWH.s_dialog_info;

create table STV2025021827__DWH.s_dialog_info
(
	hk_message_id bigint not null CONSTRAINT fk_dialog_info_dialogs REFERENCES STV2025021827__DWH.h_dialogs (hk_message_id),
	message varchar(1000),
	message_from int,
	message_to int,
	load_dt datetime,
	load_src varchar(20)
)

order by load_dt
SEGMENTED BY hk_message_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

INSERT INTO STV2025021827__DWH.s_dialog_info(hk_message_id, message,message_from,message_to,load_dt,load_src)
	select
	hd.hk_message_id,
	d.message,
	d.message_from,
	d.message_to,
	now() as load_dt,
	's3' as load_src
	from STV2025021827__STAGING.dialogs d
	left join STV2025021827__DWH.h_dialogs hd on d.message_id = hd.message_id;
	

--s_user_socdem
drop table if exists STV2025021827__DWH.s_user_socdem;

create table STV2025021827__DWH.s_user_socdem
(
	hk_user_id bigint not null CONSTRAINT fk_user_socdem_user REFERENCES STV2025021827__DWH.h_users (hk_user_id),
	country varchar(200),
 	age int,
 	load_dt datetime,
	load_src varchar(20)
)

order by load_dt
SEGMENTED BY hk_user_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

INSERT INTO STV2025021827__DWH.s_user_socdem(hk_user_id,country,age,load_dt,load_src)
	select
	hu.hk_user_id,
	u.country,
	u.age,
	now() as load_dt,
	's3' as load_src
	from STV2025021827__STAGING.users u
	left join STV2025021827__DWH.h_users hu on u.id = hu.user_id;

--s_user_chatinfo
drop table if exists STV2025021827__DWH.s_user_chatinfo;

create table STV2025021827__DWH.s_user_chatinfo
(	
	hk_user_id bigint not null CONSTRAINT fk_user_socdem_user REFERENCES STV2025021827__DWH.h_users (hk_user_id),
	chat_name varchar(200),
	load_dt datetime,
	load_src varchar(20)
)

order by load_dt
SEGMENTED BY hk_user_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

INSERT INTO STV2025021827__DWH.s_user_chatinfo(hk_user_id, chat_name,load_dt,load_src)
	select 
	hu.hk_user_id,
	u.chat_name,
	now() as load_dt,
	's3' as load_src
	from STV2025021827__STAGING.users u
	left join STV2025021827__DWH.h_users hu on u.id = hu.user_id;
	

-- 7. Ответ бизнесу

with 
	old_groups as (
		select hk_group_id
		from STV2025021827__DWH.h_groups
		order by registration_dt limit 10),

	all_messages as (
		select hk_message_id
		from STV2025021827__DWH.l_groups_dialogs
		where hk_group_id in (select hk_group_id
	                    		from STV2025021827__DWH.h_groups
                    			order by registration_dt limit 10)),
    old_users as (
		select hk_user_id
		from STV2025021827__DWH.l_user_message
		where hk_message_id in (select hk_message_id
		                        from STV2025021827__DWH.l_groups_dialogs
		                        where hk_group_id in (select hk_group_id
		                                            from STV2025021827__DWH.h_groups
		                                            order by registration_dt limit 10)))
select u.hk_user_id,
       su.age,
       ROW_NUMBER() OVER(PARTITION BY su.hk_user_id order by su.load_dt desc) as rn
from old_users u
left join STV2025021827__DWH.s_user_socdem su
--запрос, который выведет на экран количество уникальных пользователей с группировкой по возрасту, 
	--которые хотя бы раз писали сообщения в десяти самых старых сообществах соцсети. Используйте сателлит s_user_socdem.


-- проект
--создадим таблицу для нового файла

drop table if exists STV2025021827__STAGING.groups_log

create table STV2025021827__STAGING.groups_log
(
	group_id int PRIMARY KEY,
	user_id int,
	user_id_from int,
	event varchar(10),
	datetime datetime
)
order by group_id
 	SEGMENTED BY HASH(group_id) ALL NODES
 	PARTITION BY datetime::date
	GROUP BY calendar_hierarchy_day(datetime::date, 3, 2);

copy STV2025021827__STAGING.groups_log (
     group_id, user_id, user_id_from, event, datetime) 
     from 
     local 'C:\Users\Ethan\Gold_recovery_Project\project_de\sprint6\s6-lessons\group_log.csv'
     delimiter ',';
     
--4 создадим таблицу связей
    
drop table if exists STV2025021827__DWH.l_user_group_activity;

create table STV2025021827__DWH.l_user_group_activity
(
	hk_l_user_group_activity int PRIMARY KEY,
	hk_user_id bigint not null CONSTRAINT fk_user_socdem_user REFERENCES STV2025021827__DWH.h_users (hk_user_id),
	hk_group_id bigint not null CONSTRAINT fk_s_group_name_l_groups_dialogs REFERENCES STV2025021827__DWH.h_groups (hk_group_id),
	load_dt datetime,
	load_src varchar(20)
)
order by load_dt
 	SEGMENTED BY hk_l_user_group_activity ALL NODES
 	PARTITION BY load_dt::date
	GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);


--5 Создать скрипты миграции в таблицу связи

INSERT INTO STV2025021827__DWH.l_user_group_activity(l_user_group_activity, hk_user_id,hk_group_id,load_dt,load_src)

select hash(hu.hk_user_id, hg.hk_group_id),
	hu.hk_user_id,
	hg.hk_group_id,
	now() as load_dt,
	's3' as load_src
from STV2025021827__STAGING.groups_log as gl
left join STV2025021827__DWH.h_users hu on gl.user_id = hu.user_id 
left join STV2025021827__DWH.h_groups hg on gl.group_id = hg.group_id 
;


-- 6 Создать и наоплнить сателлит

drop table if exists STV2025021827__DWH.s_auth_history;

create table STV2025021827__DWH.s_auth_history
(
	hk_l_user_group_activity bigint not null CONSTRAINT fk_auth_history_user REFERENCES STV2025021827__DWH.l_user_group_activity (hk_l_user_group_activity),
	user_id_from int,
	event varchar(10),
	event_dt datetime,
	load_dt datetime,
	load_src varchar(20)
)
order by load_dt
 	SEGMENTED BY hk_l_user_group_activity ALL NODES
 	PARTITION BY load_dt::date
	GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

INSERT INTO STV2025021827__DWH.s_auth_history(hk_l_user_group_activity, user_id_from,event,event_dt,load_dt,load_src)

select
	luga.hk_l_user_group_activity,
	gl.user_id_from,
	gl.event,
	gl.datetime,
	now() as load_dt,
	's3' as load_src

from STV2025021827__STAGING.groups_log as gl
left join STV2025021827__DWH.h_groups as hg on gl.group_id = hg.group_id
left join STV2025021827__DWH.h_users as hu on gl.user_id = hu.user_id
left join STV2025021827__DWH.l_user_group_activity as luga on hg.hk_group_id = luga.hk_group_id and hu.hk_user_id = luga.hk_user_id
;

-- 7 ответ бизнесу
-- CTE 1

with user_group_messages as (
    select 
            lgd.hk_group_id, count(lum.hk_user_id) as cnt_users_in_group_with_messages
            from STV2025021827__DWH.l_groups_dialogs lgd 
            join STV2025021827__DWH.l_user_message lum on lgd.hk_message_id = lum.hk_message_id
            group by lgd.hk_group_id 
            having count(lum.hk_message_id) > 0 
)

select hk_group_id,
            cnt_users_in_group_with_messages
from user_group_messages
order by cnt_users_in_group_with_messages
limit 10
;

-- CTE 2

with user_group_log as (
    select hg.hk_group_id, count(distinct gl.user_id) as cnt_added_users
    from STV2025021827__STAGING.groups_log gl
    left join STV2025021827__DWH.h_groups hg on gl.group_id = hg.group_id
    where gl.event = 'add'
    group by hg.hk_group_id
)

select hk_group_id
            ,cnt_added_users
from user_group_log
order by cnt_added_users
limit 10
;

--ответ бизнесу
with user_group_messages as (
    select 
            lgd.hk_group_id, count(lum.hk_user_id) as cnt_users_in_group_with_messages
            from STV2025021827__DWH.l_groups_dialogs lgd 
            join STV2025021827__DWH.l_user_message lum on lgd.hk_message_id = lum.hk_message_id
            group by lgd.hk_group_id 
            having count(lum.hk_message_id) > 0 
),
user_group_log as (
    select hg.hk_group_id, count(distinct gl.user_id) as cnt_added_users
    from STV2025021827__STAGING.groups_log gl
    left join STV2025021827__DWH.h_groups hg on gl.group_id = hg.group_id
    where gl.event = 'add'
    group by hg.hk_group_id
)



select
	ugm.hk_group_id, 
	ugl.cnt_added_users, 
	ugm.cnt_users_in_group_with_messages, 
	cnt_added_users/cnt_users_in_group_with_messages as group_conversion
from user_group_messages ugm
left join user_group_log ugl on ugm.hk_group_id = ugl.hk_group_id
order by cnt_added_users/cnt_users_in_group_with_messages desc;



;