view: bigquery_conversion_model_firstplay {
  derived_table: {
    sql:

WITH
  a AS (
  SELECT
    id AS video_id,
    CASE
      WHEN series LIKE '%Heartland%' THEN 'Heartland'
      WHEN series LIKE '%Bringing Up Bates%' THEN 'Bringing Up Bates'
      ELSE 'Other'
    END AS content
  FROM
    svod_titles.titles_id_mapping),

  web AS (
  SELECT
    user_id,
    'web' AS source,
    timestamp,
    CASE
      WHEN upper(title) LIKE '%HEARTLAND%' THEN 'Heartland'
      WHEN upper(title) LIKE '%BRINGING UP BATES%' THEN 'Bringing Up Bates'
      ELSE 'Other'
    END AS content
  FROM
    javascript.firstplay
  WHERE
    user_id IS NOT NULL),

  android AS (
  SELECT
    user_id,
    'android' AS source,
    timestamp,
    content
  FROM
    android.firstplay
  INNER JOIN
    a
  ON
    firstplay.video_id=a.video_id),

  ios AS (
  SELECT
    user_id,
    'ios' AS source,
    timestamp,
    content
  FROM
    ios.firstplay
  INNER JOIN
    a
  ON
    SAFE_CAST(firstplay.video_id AS int64)=SAFE_CAST(a.video_id AS int64)),

  b AS (
  SELECT * FROM web
  UNION ALL
  SELECT * FROM android
  UNION ALL
  SELECT * FROM ios),

c as
(SELECT
  user_id,
  platform,
  frequency,
  case when campaign is not null then campaign else 'unavailable' end as campaign,
  customer_created_at,
--   date_diff(date(timestamp),date(customer_created_at),day) as daydiff,
  sum(case when content = 'Heartland' then 1 else 0 end) as watched_heartland,
  sum(case when content = 'Bringing Up Bates' then 1 else 0 end) as watched_bates,
  sum(case when content = 'Other' then 1 else 0 end) as watched_other
FROM
  b LEFT JOIN customers.subscribers ON SAFE_CAST(user_id AS int64)=SAFE_CAST(customer_id AS int64)
where date(timestamp)>=date(customer_created_at) and date(timestamp)<=date_add(date(customer_created_at), interval 14 day)
group by 1,2,3,4,5
order by user_id)

select customer_id as user_id,
       a.platform,
       a.frequency,
       case when a.campaign is not null then a.campaign else 'unavailable' end as campaign,
       a.customer_created_at,
       case when watched_heartland is null then 0 else watched_heartland end as watched_heartland,
       case when watched_bates is null then 0 else watched_bates end as watched_bates,
       case when watched_other is null then 0 else watched_other end as watched_other
from customers.subscribers as a left join c on customer_id=safe_cast(user_id as int64)

 ;;}

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

  dimension: heartland_play {
    type: number
    sql: ${TABLE}.watched_heartland ;;
  }

dimension: bates_play {
  type: number
  sql: ${TABLE}.watched_bates ;;
}

dimension: other_play {
  type: number
  sql: ${TABLE}.watched_other ;;
}


}
