view: bigquery_conversion_model_error{
  derived_table: {
    sql:
WITH
  b AS (
  SELECT user_id,timestamp FROM javascript.error where user_id is not null
  UNION ALL
  SELECT user_id,timestamp FROM android.error where user_id is not null
  UNION ALL
  SELECT user_id,timestamp FROM ios.error where user_id is not null),

c as
(SELECT
  user_id,
  platform,
  frequency,
  case when campaign is not null then campaign else 'unavailable' end as campaign,
  customer_created_at,
--   date_diff(date(timestamp),date(customer_created_at),day) as daydiff,
  count(*) as error_count
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
       case when error_count is null then 0 else error_count end as error_count
from customers.subscribers as a left join c on customer_id=safe_cast(user_id as int64)),

e as
(select avg(error_count) as e_avg, stddev(error_count) as e_std
from d)

select user_id,
platform,
frequency,
campaign,
customer_created_at,
(error_count-e_avg)/e_std as error_count
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

  dimension: frequency {
    type: string
    sql: ${TABLE}.frequency ;;
  }

  dimension: campaign {
    type: string
    sql: ${TABLE}.campaign ;;
  }

  dimension: error_count {
    type: number
    sql: ${TABLE}.error_count ;;
  }

}
