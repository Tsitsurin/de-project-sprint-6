
-- ПРОЕКТ СПРИНТ 6

-- Шаг 2. Создать таблицу group_log в Vertica
drop table if exists STV2023121121__STAGING.group_log;

truncate table STV2023121121__STAGING.group_log;

create table if not exists STV2023121121__STAGING.group_log(
	group_id bigint primary key not null,
	user_id bigint not null,
	user_id_from int,
	event varchar(6),
	"datetime" timestamp
)
ORDER BY group_id, user_id
SEGMENTED BY HASH(group_id) ALL NODES;
;

select * from STV2023121121__STAGING.group_log;



-- Шаг 4. Создать таблицу связи
drop table if exists STV2023121121__STAGING.l_user_group_activity;

create table STV2023121121__DWH.l_user_group_activity(
hk_l_user_group_activity bigint primary key,
hk_user_id bigint not null CONSTRAINT fk_l_user_group_activity_user REFERENCES STV2023121121__DWH.h_users(hk_user_id),
hk_group_id bigint not null CONSTRAINT fk_l_user_group_activity_group REFERENCES STV2023121121__DWH.h_groups(hk_group_id),
load_dt DATETIME,
load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_user_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);


-- Шаг 5. Создать скрипты миграции в таблицу связи
INSERT INTO STV2023121121__DWH.l_user_group_activity(hk_l_user_group_activity, hk_user_id, hk_group_id, load_dt, load_src)
select distinct
	hash(hu.hk_user_id, g.hk_group_id),
	hu.hk_user_id,
	g.hk_group_id,
	now() as load_dt,
	's3' as load_src
from STV2023121121__STAGING.group_log as gl
left join STV2023121121__DWH.h_users hu on gl.user_id = hu.user_id
left join STV2023121121__DWH.h_groups g on gl.group_id = g.group_id 
where hash(hu.hk_user_id, g.hk_group_id) not in (select hk_l_user_group_activity from STV2023121121__DWH.l_user_group_activity);





-- Шаг 6. Создать и наполнить сателлит
create table STV2023121121__DWH.s_auth_history(
	hk_l_user_group_activity bigint not null CONSTRAINT fk_s_auth_history_l_user_group_activity REFERENCES STV2023121121__DWH.l_user_group_activity (hk_l_user_group_activity), 
	user_id_from int,
	event varchar(6),
	event_dt timestamp,
	load_dt datetime,
	load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_l_user_group_activity all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);
	

INSERT INTO STV2023121121__DWH.s_auth_history(hk_l_user_group_activity, user_id_from, event, event_dt, load_dt, load_src)
select 
	luga.hk_l_user_group_activity,
	gl.user_id_from,
	gl.event,
	gl."datetime" as event_dt,
	now() as load_dt,
	's3' as load_src
from STV2023121121__STAGING.group_log as gl
left join STV2023121121__DWH.h_groups as hg on gl.group_id = hg.group_id
left join STV2023121121__DWH.h_users as hu on gl.user_id = hu.user_id
left join STV2023121121__DWH.l_user_group_activity as luga on hg.hk_group_id = luga.hk_group_id and hu.hk_user_id = luga.hk_user_id
;






-- Шаг 7.1. Подготовить CTE user_group_messages
with user_group_messages as (
	select 
		hg.hk_group_id,
		count(distinct hs.user_id) as cnt_users_in_group_with_messages
	from
	    STV2023121121__DWH.h_groups hg 
	join
	    STV2023121121__DWH.l_user_group_activity luga on hg.hk_group_id = luga.hk_group_id
	join
		STV2023121121__DWH.h_users hs on luga.hk_user_id = hs.hk_user_id 
	join
		STV2023121121__DWH.l_user_message lum on hs.hk_user_id = lum.hk_user_id 
	join
		STV2023121121__DWH.h_dialogs hd on lum.hk_message_id = hd.hk_message_id
	group by
		hg.hk_group_id
)
select hk_group_id,
            cnt_users_in_group_with_messages
from user_group_messages
order by cnt_users_in_group_with_messages
limit 10
;



-- Шаг 7.2. Подготовить CTE user_group_log
with user_group_log as (
	select 
		hg.hk_group_id,
		count(distinct hs.user_id) as cnt_added_users
	from
		(
			select
				hk_group_id, registration_dt
			from 
				STV2023121121__DWH.h_groups 
			order by 
				registration_dt
			limit 10
		)hg 
	join
		STV2023121121__DWH.l_user_group_activity luga on hg.hk_group_id = luga.hk_group_id
	join
		STV2023121121__DWH.s_auth_history sah on luga.hk_l_user_group_activity = sah.hk_l_user_group_activity and sah.event = 'add'
	join
		STV2023121121__DWH.h_users hs on luga.hk_user_id = hs.hk_user_id 
	group by
		hg.hk_group_id
)
select 
	hk_group_id,
	cnt_added_users
from user_group_log
order by cnt_added_users
limit 10
;



-- Шаг 7.3. Написать запрос и ответить на вопрос бизнеса
with user_group_log as (
	select 
		hg.hk_group_id,
		count(distinct hs.user_id) as cnt_added_users
	from
		(
			select
				hk_group_id, registration_dt
			from 
				STV2023121121__DWH.h_groups 
			order by 
				registration_dt
			limit 10
		)hg 
	join
		STV2023121121__DWH.l_user_group_activity luga on hg.hk_group_id = luga.hk_group_id
	join
		STV2023121121__DWH.s_auth_history sah on luga.hk_l_user_group_activity = sah.hk_l_user_group_activity and sah.event = 'add'
	join
		STV2023121121__DWH.h_users hs on luga.hk_user_id = hs.hk_user_id 
	group by
		hg.hk_group_id
)
,user_group_messages as (
	select 
		hg.hk_group_id,
		count(distinct hs.user_id) as cnt_users_in_group_with_messages
	from
	    STV2023121121__DWH.h_groups hg 
	join
	    STV2023121121__DWH.l_user_group_activity luga on hg.hk_group_id = luga.hk_group_id
	join
		STV2023121121__DWH.h_users hs on luga.hk_user_id = hs.hk_user_id 
	join
		STV2023121121__DWH.l_user_message lum on hs.hk_user_id = lum.hk_user_id 
	join
		STV2023121121__DWH.h_dialogs hd on lum.hk_message_id = hd.hk_message_id
	group by
		hg.hk_group_id
)
select  
	ugl.hk_group_id, 
	ugl.cnt_added_users, 
	ugm.cnt_users_in_group_with_messages, 
	(ugm.cnt_users_in_group_with_messages / ugl.cnt_added_users) as group_conversion
from user_group_log as ugl
left join user_group_messages as ugm on ugl.hk_group_id = ugm.hk_group_id
order by ugm.cnt_users_in_group_with_messages / ugl.cnt_added_users desc;


