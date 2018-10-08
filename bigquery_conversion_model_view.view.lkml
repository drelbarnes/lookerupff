view: bigquery_conversion_model_view {
  derived_table: {
    sql: WITH
  b AS (
  SELECT safe_cast(user_id as int64) as user_id,timestamp FROM javascript.pages where user_id is not null
  UNION ALL
  SELECT safe_cast(user_id as int64) as user_id,timestamp FROM android.view where user_id is not null
  UNION ALL
  SELECT user_id,timestamp FROM ios.view where user_id is not null),

c as
(SELECT
  user_id,
  platform,
  frequency,
  case when campaign is not null then campaign else 'unavailable' end as campaign,
  customer_created_at,
--   date_diff(date(timestamp),date(customer_created_at),day) as daydiff,
  count(*) as view_count
FROM
  customers.subscribers left JOIN b ON SAFE_CAST(user_id AS int64)=SAFE_CAST(customer_id AS int64)
where date(timestamp)>=date(customer_created_at) and date(timestamp)<=date_add(date(customer_created_at), interval 14 day)
group by 1,2,3,4,5
order by user_id)

select customer_id as user_id,
       a.platform,
       a.frequency,
       case when a.campaign is not null then a.campaign else 'unavailable' end as campaign,
       a.customer_created_at,
       case when view_count is null then 0 else view_count end as view_count
from customers.subscribers as a left join c on customer_id=safe_cast(user_id as int64);;
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

  dimension: view_count {
    type: number
    sql: ${TABLE}.view_count ;;
  }

}
