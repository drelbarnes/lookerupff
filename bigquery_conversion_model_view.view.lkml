view: bigquery_conversion_model_view {
  derived_table: {
    sql:
 WITH
  b AS (
  SELECT  user_id,timestamp FROM javascript.view where user_id is not null
  UNION ALL
  SELECT user_id,timestamp FROM android.view where user_id is not null
  UNION ALL
  SELECT cast(user_id as string) as user_id,timestamp FROM ios.view where user_id is not null
  union all
  SELECT user_id,timestamp FROM roku.view where user_id is not null),

purchase_event as
(select distinct  user_id, created_at, platform
from http_api.purchase_event
where date(created_at)>'2018-10-31'),

d as
(SELECT
  a.user_id,
  platform,
  created_at,
--   date_diff(date(timestamp),date(customer_created_at),day) as daydiff,
  count(*) as view_count
FROM
  purchase_event as a left JOIN b ON SAFE_CAST(b.user_id AS int64)=SAFE_CAST(a.user_id AS int64) and date(b.timestamp)>=date(created_at) and date(b.timestamp)<=date_add(date(created_at), interval 14 day)
group by 1,2,3),


e as
(select max(view_count) as rwl_max, min(view_count) as rwl_min
from d)

select user_id,
platform,
created_at as customer_created_at,
(view_count-rwl_min)/(rwl_max-rwl_min) as view_count
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


  dimension: view_count {
    type: number
    sql: ${TABLE}.view_count ;;
  }

}
