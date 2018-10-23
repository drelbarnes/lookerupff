view: bigquery_conversion_model_removewatchlist {
derived_table: {
  sql:
WITH
  b AS (
  SELECT user_id,timestamp FROM javascript.removewatchlist where user_id is not null
  UNION ALL
  SELECT user_id,timestamp FROM android.removewatchlist where user_id is not null
  UNION ALL
  SELECT user_id,timestamp FROM ios.removewatchlist where user_id is not null),

c as
(SELECT
  user_id,
  platform,
  frequency,
  case when campaign is not null then campaign else 'unavailable' end as campaign,
  customer_created_at,
--   date_diff(date(timestamp),date(customer_created_at),day) as daydiff,
  count(*) as removewatchlist_count
FROM
  customers.subscribers left JOIN b ON SAFE_CAST(user_id AS int64)=SAFE_CAST(customer_id AS int64)
where date(timestamp)>=date(customer_created_at) and date(timestamp)<=date_add(date(customer_created_at), interval 14 day)
group by 1,2,3,4,5
order by user_id),

d as
(select customer_id as user_id,
       a.platform,
       a.frequency,
       case when a.campaign is not null then a.campaign else 'unavailable' end as campaign,
       a.customer_created_at,
       case when removewatchlist_count is null then 0 else removewatchlist_count end as removewatchlist_count
from customers.subscribers as a left join c on customer_id=safe_cast(user_id as int64)),

e as
(select max(removewatchlist_count) as r_max, min(removewatchlist_count) as r_min
from d)

select user_id,
platform,
frequency,
campaign,
customer_created_at,
(removewatchlist_count-r_min)/(r_max-r_min) as removewatchlist_count
from d,e
order by removewatchlist_count desc


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

  dimension: frequency {
    type: string
    sql: ${TABLE}.frequency ;;
  }

  dimension: campaign {
    type: string
    sql: ${TABLE}.campaign ;;
  }

  dimension: removewatchlist_count {
    type: number
    sql: ${TABLE}.removewatchlist_count ;;
  }

}
