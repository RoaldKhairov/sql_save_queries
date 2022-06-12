---------------------------------promouters user-------------------------------------------
--Код берет данные о привелеченых промоутерами из таблицы анкет. 
--Отбирает только анкеты с уникальными номерами по минимальному времени заполнения для исключения дубликатов анкет.

with promo_contacts as (
  SELECT 
    RIGHT(contact, 9) as contact,
    contact as contact_full,
    promoter,
    your_area,
    your_squad,
    min(timestamp) as timestamp
  FROM `unreasonably-good.imports.offlline_promo_bq`
    where contact is not null
  group by 
    RIGHT(contact, 9),
    contact_full,
    promoter,
    your_area,
    your_squad
),

min_time_promo_contacts as (
  select 
    *,
    min(timestamp) over(partition by contact) as min_timestamp
  from promo_contacts
),

only_first_promo as (
  select 
    *
  from min_time_promo_contacts
  where min_timestamp = timestamp
),
 
---------------------------------all user with promouters flg---------------------------------
--Берем всех пользователей из анкет промоутеров и объединяем с данными бекенда по номеру телефона и по правилу, что время создания профиля за 24 до или после заполнения анкеты. 
--Правило такое выработано в ходе дискуссии :)

all_user as (
  select 
    a._id as user_id,
    a.created_at as created_at_user,
    a.phone,
    b.*
  from `unreasonably-good.fres_mongodb_us.users` a
    left join only_first_promo  b
      on RIGHT(b.contact, 9) = RIGHT(a.phone, 9)
        and datetime(a.created_at, 'Asia/Dubai') >= DATETIME_SUB(datetime(Timestamp), INTERVAL 24 hour)
        and datetime(a.created_at, 'Asia/Dubai') <= DATETIME_ADD(datetime(Timestamp), INTERVAL 24 hour) 
),

---------------------------------all user with appslyer attribution----------------------------
--Достаем значения параметра user_id пользователя из сырых событий appsflyer (не органика). 
--У каждого события приписывается источник установки. Костыльно, но работает. 
--Источник у пользователя считаем по самой ранней установке

row_data as (
  SELECT 
    JSON_EXTRACT_SCALAR(event_value , '$.af_customer_id') AS customer_id,
    install_time,
    media_source,
    channel,
    campaign
  FROM `unreasonably-good.bq_appsflyer_ios.in_app_events_report`
  where json_extract(event_value , '$.af_customer_id') is not null
  group by 
    JSON_EXTRACT_SCALAR(event_value , '$.af_customer_id'),
    install_time,
    media_source,
    channel,
    campaign
  
  union all
  
  SELECT 
    JSON_EXTRACT_SCALAR(event_value , '$.af_customer_id') AS customer_id,
    install_time,
    media_source,
    channel,
    campaign
  FROM `unreasonably-good.bq_appsflyer_android.in_app_events_report`
  where json_extract(event_value , '$.af_customer_id') is not null
  group by 
    JSON_EXTRACT_SCALAR(event_value , '$.af_customer_id'),
    install_time,
    media_source,
    channel,
    campaign
),

min_max_time as (
  select 
    *,
    max(install_time) over(partition by customer_id) as max_install_time,
    min(install_time) over(partition by customer_id) as min_install_time
  from row_data
),

appsflyer_final as (
  select 
    customer_id,
    install_time,
    media_source,
    channel,
    campaign
  from min_max_time
    where min_install_time = install_time
),
    
----------------------------------------final-------------------------------------------------    
    
all_data as (
  select 
    a.*,
    b.*
  from all_user a
    left join appsflyer_final b
      on b.customer_id = a.user_id
)


select 
  user_id,
  created_at_user,
  phone,
  case when (promoter is not null and (date(timestamp) < date(install_time))) 
            or ((promoter is not null and install_time is null)) 
            or (promoter is not null and media_source = 'restricted') 
            then timestamp(timestamp)
        else timestamp(install_time) end install_time,
  
  case when (promoter is not null and (date(timestamp) < date(install_time))) 
            or ((promoter is not null and install_time is null)) 
            or (promoter is not null and media_source = 'restricted') 
            then 'Offline_Promo_Base' 
       when promoter is null and media_source is null then 'Organic' 
        else media_source end media_source,
      
  case when (your_area is not null and (date(timestamp) < date(install_time))) 
            or ((your_area is not null and install_time is null))         
            or (your_area is not null and media_source = 'restricted') 
            then your_area
        else campaign end campaign,
  
  case when (your_squad is not null and (date(timestamp) < date(install_time))) 
            or ((your_squad is not null and install_time is null))         
            or (your_squad is not null and media_source = 'restricted') 
            then your_squad
        else channel end channel
  
from all_data
