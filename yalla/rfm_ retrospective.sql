declare dt date default DATE("2021-10-31",'Asia/Dubai');  ### start date
      WHILE
  dt < DATE('2021-12-19','Asia/Dubai') DO    ### end date

INSERT INTO  report_temp.rfm

with order_list as (
  select 
    user_id,
    max(case when state = 'delivered' then date(created_at, 'Asia/Dubai') else null end) as last_order_day,
    count(distinct case when state = 'delivered' then _id else null end) as cnt_orders,
    sum(case when state = 'delivered' then total else null end) as total
  from `unreasonably-good.fres_mongodb_us.orders`
    where date(created_at,'Asia/Dubai') <= dt
  group by 1
 ),

final_data as (
  select 
    a._id as user_id,
    date(a.created_at, 'Asia/Dubai') as created_at,
    a.email,
    a.first_name,
    a.last_name,
    a.phone,
    date_diff(dt, b.last_order_day, DAY) AS days_diff_last_day,
    b.last_order_day,
    b.cnt_orders,
    b.total
  from `unreasonably-good.fres_mongodb_us.users` a 
    left join order_list b 
      on a._id = b.user_id
  where cnt_orders > 0
    and date(a.created_at,'Asia/Dubai') <= dt
),

------------------------------------------- rfm pre_data -------------------------------

rfm_data as (
  select
    *,
    case when days_diff_last_day <= 7 then '3'
    when days_diff_last_day >= 8 and days_diff_last_day <= 21 then '2'
    when days_diff_last_day > 21 then '1'
    end as R,
    case when cnt_orders >= 5 then '3'
    when cnt_orders >= 2 and cnt_orders <= 4 then '2'
    when cnt_orders = 1 then '1'
    end as F,
    case when total >= 125 then '3'
    when total < 125 and total >=60 then '2'
    when total <60 then '1'
    end as M
  from final_data
),

rfm_final as (
  select 
    *,
    cast(R as int64)+cast(F as int64)+cast(M as int64) as sum,
    case when R = '1' and F = '1' and M in ('1','2','3') then '1 group'
    when R in ('2','3') and F = '1' and M in ('1','2','3') then '2 group'
    when R = '1' and F = '2' and M in ('1','2','3') then '3 group'
    when R in ('2','3') and F = '2' and M in ('1','2','3') then '4 group'
    when R = '1' and F = '3' and M in ('1','2','3') then '5 group'
    when R = '3' and F = '3' and M ='3' then '7 group'
    when R in ('2','3') and F = '3' and M in ('1','2','3') then '6 group'
    else 'error' end as type
  from rfm_data
)

select 
  dt as execution_date,
  user_id,
  created_at,
  email,
  first_name,	
  last_name,
  phone,	
  days_diff_last_day,	
  last_order_day,	
  cnt_orders,
  cast(total as int64) as total,
  R,
  F,	
  M,
  sum,	
  type,
from rfm_final;

SET dt = DATE_ADD(dt, INTERVAL 1 day);  ### шаг выгрузки
END WHILE
