with pre_event_data_1 as (
  select 
    date_trunc(date(TIMESTAMP_MICROS(event_timestamp), 'Asia/Dubai'), week(Friday)) as week, 
    user_pseudo_id,
    event_name,
    (select value.string_value from unnest(event_params) where key = 'firebase_screen') as firebase_screen,
    (select value.string_value from unnest(event_params) where key = 'order_id') as order_id,
    date_trunc(date(TIMESTAMP_MICROS(user_first_touch_timestamp), 'Asia/Dubai'), week(Friday)) as week_first_open
  from `unreasonably-good.analytics_288568027.events_*`
  where _TABLE_SUFFIX > 'REGEXP_REPLACE(CAST(DATE_SUB(CURRENT_DATE(), INTERVAL 45 DAY) AS STRING), "-", "")
    and event_name in ('first_open', 'welcome_offer_show', 'welcome_offer_claim_click', 'screen_view', 'add_to_cart', 'add_to_card', 'purchase')
),

------------------------------------------------------Данные о  успешных покупках-------------------------------------------------------

order_data_success as (
  select 
	  _id,
    case when line_items like '%61c07c09f48b4f72c67e7574%' then '1'
      else '0' end as wp_success_flg
  from `unreasonably-good.fres_mongodb_us.orders`
  where state = 'delivered'
),

------------------------------------------------------Данные о всех покупках промопака-------------------------------------------------------

order_data_all as (
  select
  	_id,
    case when line_items like '%61c07c09f48b4f72c67e7574%' then '1'
      else '0' end as wp_all_flg
  from `unreasonably-good.fres_mongodb_us.orders`
),

pre_event_data_2 as (
  select 
    week,
    week_first_open,
    user_pseudo_id,
    case when event_name = 'screen_view' and firebase_screen = '/categories' then 'showcase'
      when event_name = 'purchase' and wp_success_flg = '1' then 'purchase_wp_delivered'
      when event_name = 'purchase' and wp_success_flg = '0' then 'purchase_without_wp_delivered'
        else event_name end as event_name,
    case when event_name = 'purchase' and wp_all_flg = '1' then 'purchase_wp_all'
      when event_name = 'purchase' and wp_all_flg = '0' then 'purchase_without_wp_all'
        else event_name end as event_name_for_all,
    case when event_name = 'purchase' and (wp_all_flg = '1' or wp_all_flg = '0') then 'purchase_all'
      else event_name end as event_name_for_all2,
    case when event_name = 'purchase' and (wp_success_flg = '1' or wp_success_flg = '0') then 'purchase_all_delivered'
      else event_name end as event_name_for_all_delivered,
  from pre_event_data_1 a
    left join order_data_success b
      on a.order_id = b._id
    left join order_data_all c
      on a.order_id = c._id
),
  
final_pre_data_1 as (
  select 
    week,
    count(distinct case when event_name = 'welcome_offer_show' then user_pseudo_id else null end) as welcome_offer_show,
    count(distinct case when event_name = 'welcome_offer_claim_click' then user_pseudo_id else null end) as welcome_offer_claim_click,
    count(distinct case when event_name_for_all = 'purchase_wp_all' then user_pseudo_id else null end) as first_purchase_wp,
    count(distinct case when event_name = 'purchase_wp_delivered' then user_pseudo_id else null end) as first_purchase_wp_delivered,
    
    count(distinct case when event_name = 'showcase' then user_pseudo_id else null end) as showcase,
    count(distinct case when event_name_for_all = 'add_to_cart' or event_name_for_all = 'add_to_card' then user_pseudo_id else null end) as add_to_cart,
    count(distinct case when event_name_for_all2 = 'purchase_all' then user_pseudo_id else null end) as purchase_all,
    count(distinct case when event_name_for_all_delivered = 'purchase_all_delivered' then user_pseudo_id else null end) as purchase_all_delivered,
  from pre_event_data_2
  where week_first_open = week
  group by 1
),

final_pre_data_2 as (
  select 
    week,
    case when welcome_offer_show = 0 then null else welcome_offer_show end as welcome_offer_show,
    case when welcome_offer_claim_click = 0 then null else welcome_offer_claim_click end as welcome_offer_claim_click,
    case when first_purchase_wp = 0 then null else first_purchase_wp end as first_purchase_wp,
    case when first_purchase_wp_delivered = 0 then null else first_purchase_wp_delivered end as first_purchase_wp_delivered,
    showcase,
    add_to_cart,
    purchase_all,
    purchase_all_delivered	
  from final_pre_data_1
)

select 
  *,
  welcome_offer_claim_click / welcome_offer_show * 100 as wp_offer_to_click,
  first_purchase_wp / welcome_offer_claim_click * 100   as wp_click_to_purchase,
  first_purchase_wp / 	welcome_offer_show * 100	 as wp_offer_to_purchase,
  add_to_cart / showcase * 100 as main_to_add_cart,
  purchase_all / add_to_cart * 100 as add_cart_to_purchase,
  purchase_all / showcase * 100 as main_to_purchase,
  purchase_all_delivered / purchase_all * 100 as part_failed_delivered
from final_pre_data_2
