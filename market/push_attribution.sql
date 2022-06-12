-----------------------Датамарт для дашборда атрибуции действий пользователя по пуш уведомлению-----------------------

with data as(
select
 user_pseudo_id,
 event_timestamp,
 platform,
 event_name,
 event_params,
 (select value.string_value from unnest(event_params) where key = 'after_push') as after_push,
 (select value.string_value from unnest(event_params) where key = 'utm_source') as utm_source,
 (select value.string_value from unnest(event_params) where key = 'utm_medium') as utm_medium,
 (select value.string_value from unnest(event_params) where key = 'link') as link,
 (select value.string_value from unnest(event_params) where key = 'utm_campaign') as utm_campaign,
 CASE 
        WHEN TIMESTAMP_DIFF(lag(timestamp_micros(event_timestamp),1) OVER(PARTITION BY  user_pseudo_id ORDER BY timestamp_micros(event_timestamp)),timestamp_micros(event_timestamp),MINUTE) < -30 THEN '1'
        WHEN TIMESTAMP_DIFF(lag(timestamp_micros(event_timestamp),1) OVER(PARTITION BY  user_pseudo_id ORDER BY timestamp_micros(event_timestamp)),timestamp_micros(event_timestamp),MINUTE) is null THEN '1'
          ELSE '0' END as start_session,
date(TIMESTAMP_MICROS(event_timestamp), "Asia/Almaty") as table
from `market-prod.analytics_153901123.events_*`
where _TABLE_SUFFIX between '20210201' and REGEXP_REPLACE(CAST(DATE_SUB(CURRENT_DATE('Asia/Almaty') , INTERVAL 1 DAY) AS STRING), "-", "") --jj
  and platform in ('ANDROID', 'IOS')
  and app_info.version not like '%debug%'
  and app_info.id in ('kz.market', 'kz.market.advapp') -- проверить проект
),

sessions as (
select *,
CONCAT(table,'-',user_pseudo_id,'-' ,CAST(sum(CAST(start_session as int64)) over (PARTITION BY user_pseudo_id order by timestamp_micros(event_timestamp)) as STRING ))as session_id
from data
order by 1,4
),

push as(
select 
table,
utm_source,
utm_medium,
utm_campaign,
session_id,
platform,
user_pseudo_id,
event_name,
event_timestamp,
case when after_push = '1' then 'AppMetrica'
     when after_push = '0' then 'OneSignal_'
     else null
     end as push_service
from sessions
where event_name = 'deeplink' and (after_push = '1' or link like 'marketkzapp:%') 
),

advert_view as (
select 
table,
session_id,
user_pseudo_id,
platform,
event_name,
event_timestamp

from sessions
where event_name = 'view_ad'
),

phone as (
select 
table,
session_id,
user_pseudo_id,
platform,
event_name,
event_timestamp

from sessions
where event_name = 'phone_show'
),

message as (
select 
table,
session_id,
user_pseudo_id,
event_name,
event_timestamp,
platform

from sessions
where event_name = 'message_send' and (select value.string_value from unnest(event_params) where key = 'message_source') = 'advert'
),

contact as (
select 
table,
session_id,
user_pseudo_id,
event_name,
event_timestamp,
platform

from sessions
where (event_name = 'message_send' and (select value.string_value from unnest(event_params) where key = 'message_source') = 'advert')
or event_name = 'phone_show'
),

new_ads as (
select 
table,
session_id,
user_pseudo_id,
event_name,
event_timestamp,
platform

from sessions
where event_name = 'new_advert_success'
),

phone_push1 as(
select
a.*,
utm_source,
utm_medium,
utm_campaign,
push_service,
b.event_timestamp as event_timestamp2

from phone a inner join push b on (a.session_id = b.session_id and a.event_timestamp > b.event_timestamp)
),

message_push1 as(
select
a.*,
utm_source,
utm_medium,
utm_campaign,
push_service,
b.event_timestamp as event_timestamp2

from message a inner join push b on (a.session_id = b.session_id and a.event_timestamp > b.event_timestamp)
),

contact_push1 as(
select
a.*,
utm_source,
utm_medium,
utm_campaign,
push_service,
b.event_timestamp as event_timestamp2

from contact a inner join push b on (a.session_id = b.session_id and a.event_timestamp > b.event_timestamp)
),

view_ad_push1 as(
select
a.*,
utm_source,
utm_medium,
utm_campaign,
push_service,
b.event_timestamp as event_timestamp2

from advert_view a inner join push b on (a.session_id = b.session_id and a.event_timestamp > b.event_timestamp)
),

new_ad_push1 as(
select
a.*,
utm_source,
utm_medium,
utm_campaign,
push_service,
b.event_timestamp as event_timestamp2

from new_ads a inner join push b on (a.session_id = b.session_id and a.event_timestamp > b.event_timestamp)
)

select 
coalesce(a.platform,b.platform,c.platform,d.platform, e.platform) as platform,
coalesce(a.push_service,b.push_service,c.push_service,d.push_service,e.push_service) as push_service,
coalesce(a.table,b.table,c.table,d.table,e.table) as table,
coalesce(a.utm_source,b.utm_source,c.utm_source,d.utm_source,e.utm_source) as utm_source,
coalesce(a.utm_medium,b.utm_medium,c.utm_medium,d.utm_medium,e.utm_medium) as utm_medium,
coalesce(a.utm_campaign,b.utm_campaign,c.utm_campaign,d.utm_campaign,e.utm_campaign) as utm_campaign,
phone_users,
phone_hits,
message_users,
message_hits,
contact_users,
contact_hits,
new_ad_hits,
new_ad_users,
view_ad_users,
view_ad_hits
from 
  (select
    platform,
    table,
    utm_source,
    utm_medium,
    utm_campaign,
    push_service,
    count(distinct user_pseudo_id) as phone_users,
    count(event_name) as phone_hits
   from phone_push1
   group by 1,2,3,4,5,6) a
   
   full outer join 
   
   (select
    platform,
    table,
    utm_source,
    utm_medium,
    utm_campaign,
    push_service,
    count(distinct user_pseudo_id) as message_users,
    count(event_name) as message_hits
   from message_push1
   group by 1,2,3,4,5,6) b
   
   on (a.table = b.table and a.platform = b.platform and a.utm_source = b.utm_source and a.utm_medium = b.utm_medium and a.utm_campaign = b.utm_campaign and a.push_service = b.push_service)
   
   full outer join 
   
   (select
    platform,
    table,
    utm_source,
    utm_medium,
    utm_campaign,
    push_service,
    count(distinct user_pseudo_id) as new_ad_users,  
    count(event_name) as new_ad_hits
   from new_ad_push1
   group by 1,2,3,4,5,6) c
   
   on (b.table = c.table and b.platform = c.platform and b.utm_source = c.utm_source and b.utm_medium = c.utm_medium and b.utm_campaign = c.utm_campaign and  c.push_service = b.push_service)  
   
   full outer join 
   
   (select
    platform,
    table,
    utm_source,
    utm_medium,
    utm_campaign,
    push_service,
    count(distinct user_pseudo_id) as view_ad_users,  
    count(event_name) as view_ad_hits
   from view_ad_push1
   group by 1,2,3,4,5,6) d
   
   on (c.table = d.table and c.platform = d.platform and c.utm_source = d.utm_source and c.utm_medium = d.utm_medium and c.utm_campaign = d.utm_campaign and c.push_service = d.push_service)
   
   full outer join ----
   
   (select
    platform,
    table,
    utm_source,
    utm_medium,
    utm_campaign,
    push_service,
    count(distinct user_pseudo_id) as contact_users,  
    count(event_name) as contact_hits
   from contact_push1
   group by 1,2,3,4,5,6) e
   
   on (d.table = e.table and d.platform = e.platform and d.utm_source = e.utm_source and d.utm_medium = e.utm_medium and d.utm_campaign = e.utm_campaign and d.push_service = e.push_service)
