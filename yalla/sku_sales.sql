# Таблица создана  по заказу маркетинга для анализа продаж продуктов. Применяется в дашборде sales


with pre_data as (
  SELECT 
    o._id as order_id,
    datetime(created_at, 'Asia/Dubai') as created_at,
    date(created_at, 'Asia/Dubai') as date,
    date_trunc(date(created_at, 'Asia/Dubai'), week(sunday)) as week,
    o.order_num,
    user_id,
    json_extract_scalar(l, '$.product_id') as product_id, 
    loc.location_code,
    json_extract_scalar(l, '$.price_per_unit') as price_per_unit, 
    json_extract_scalar(l, '$.quantity') as quantity, 
    cast(json_extract_scalar(l, '$.cost_price') as float64)  as cost_price,
    # /// Строка ниже ищет предыдущую дату покупки товара у пользователя ///
    LAG(date(created_at, 'Asia/Dubai')) OVER (PARTITION BY user_id, json_extract_scalar(l, '$.product_id') ORDER BY created_at ASC) AS preceding_date_purchase,
    json_extract_scalar(l, '$.sale') as sale, 
    promo,
    json_extract_scalar(l, '$.source') as source,  
  FROM `unreasonably-good.fres_mongodb_us.orders` o
    left join unnest(json_extract_array(line_items)) as l
    left join `unreasonably-good.fres_mongodb_us.locations` as loc on loc._id = o.location_id
  where state = 'delivered'
),

# /// Достаем субкатегорию продуктов
products_subcategory as (
  select
    _id as product_id,
    subcategory
  from 
    `unreasonably-good.fres_mongodb_us.products`,
    UNNEST(SPLIT(REPLACE(REPLACE(subcategories, '["', ''), '"]', ''), '","')) subcategory
  where __hevo__marked_deleted is false
),

# /// Достаем субкатегорию и категорию продуктов
categories as (
  select
    c._id as category_id,
    case when c1.title is not null then
        REPLACE(json_extract(c.title , '$.en'), '"', '') 
      else NULL 
        end sub_category,
    case when c1.title is null then
        REPLACE(json_extract(c.title , '$.en'), '"', '') 
      else REPLACE(json_extract(c1.title , '$.en'), '"', '') 
        end as category
  from `unreasonably-good.fres_mongodb_us.categories` c
    left join `unreasonably-good.fres_mongodb_us.categories` c1 on c.parent_id = c1._id
  where c.__hevo__marked_deleted is false
),

# /// Порядковый номер заказа пользователя
orders_numbers as (
  select 
    created_at,
    user_id,
    order_num,
    row_number() over(partition by user_id order by created_at) as number_order
  FROM `unreasonably-good.fres_mongodb_us.orders`
  where state = 'delivered'
  ),

final_data as (
  select 
    a.order_id,
    a.created_at,
    a.date,
    a.week,
    a.date + 7 as prev_week,
    b.number_order,
    min(a.week) over(partition by a.user_id, p.sku) as first_sku_week,
    p.sku,
    a.order_num,
    a.user_id,
    c.sub_category,
    c.category,
    a.location_code,
    cast(a.price_per_unit as float64) as pseudo_price_per_unit,
    cast(a.quantity as int64) as quantity ,
    cast(a.price_per_unit as float64) * cast(a.quantity as float64) as total_pseudo_price_per_unit,
    a.cost_price,
    cast(a.cost_price as float64) * cast(a.quantity as float64) as total_cost_price_per_unit,
    a.preceding_date_purchase,
    case when a.preceding_date_purchase is not null then 'Repeat'
      else 'New'
        end as repeat_purchase_flg,
    cast(price_per_unit as float64) - a.cost_price as pseudo_margin, -- Это псевдо маржа
    case when a.sale is not null then 1
      else 0 
        end as sale_flg,
    promo,
    case when promo is not null then 1
      else 0 
        end as promo_flg,
    source
  from pre_data a
    left join orders_numbers b on a.order_num = b.order_num
    left join products_subcategory s on a.product_id = s.product_id
    left join categories c on s.subcategory = c.category_id
    left join `unreasonably-good.fres_mongodb_us.products` p on a.product_id = p._id)

select *
from final_data
