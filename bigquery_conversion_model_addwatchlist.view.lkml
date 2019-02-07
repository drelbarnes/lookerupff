view: bigquery_conversion_model_addwatchlist {
  derived_table: {
    sql:
WITH
  b AS (
  SELECT user_id,timestamp FROM javascript.addwatchlist where user_id is not null
  UNION ALL
  SELECT user_id,timestamp FROM android.addwatchlist where user_id is not null
  UNION ALL
  SELECT user_id,timestamp FROM ios.addwatchlist where user_id is not null),

purchase_event as
(with
b as
(select user_id, min(received_at) as received_at
from http_api.purchase_event
where topic in ('customer.product.free_trial_created','customer.product.created','customer.created') and date(created_at)=date(received_at) and date(created_at)>'2018-10-31'
group by 1)

select a.user_id, a.platform, created_at
from b inner join http_api.purchase_event as a on a.user_id=b.user_id and a.received_at=b.received_at
where topic in ('customer.product.free_trial_created','customer.product.created','customer.created') and date(created_at)=date(a.received_at) and date(created_at)>'2018-10-31'),

c as
(SELECT
  a.user_id,
  platform,
  created_at,
--   date_diff(date(timestamp),date(customer_created_at),day) as daydiff,
  count(*) as addwatchlist_count
FROM
  purchase_event as a left JOIN b ON SAFE_CAST(b.user_id AS int64)=SAFE_CAST(a.user_id AS int64)
where date(b.timestamp)>=date(created_at) and date(b.timestamp)<=date_add(date(created_at), interval 14 day)
group by 1,2,3),

d as
(select a.user_id,
       a.platform,
       a.created_at,
       case when addwatchlist_count is null then 0 else addwatchlist_count end as addwatchlist_count
from purchase_event as a left join c on a.user_id=c.user_id
where a.user_id<>'0'),

e as
(select max(addwatchlist_count) as awl_max, min(addwatchlist_count) as awl_min
from d)

select user_id,
platform,
created_at as customer_created_at,
(addwatchlist_count-awl_min)/(awl_max-awl_min) as addwatchlist_count
from d,e

 ;;
  }

  dimension: user_id {
    primary_key: yes
    tags: ["user_id"]
    type: string
    sql: ${TABLE}.user_id ;;
  }

  dimension_group: customer_created_at {
    type: time
    timeframes: [
      raw,
      time,
      date,
      week,
      month,
      quarter,
      year
    ]
    sql: ${TABLE}.customer_created_at ;;
  }

  dimension: platform {
    type: string
    sql: ${TABLE}.platform ;;
  }


dimension: addwatchlist_count {
  type: number
  sql: ${TABLE}.addwatchlist_count ;;
}

measure: addwatchlist {
  type: sum
  sql: ${addwatchlist_count} ;;
}

 }
