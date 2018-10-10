view: bigquery_conversion_model_timeupdate {
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
    END AS content,
    current_time as timecode
  FROM
    javascript.timeupdate
  WHERE
    user_id IS NOT NULL),

  android AS (
  SELECT
    user_id,
    'android' AS source,
    timestamp,
    content,
    timecode
  FROM
    android.timeupdate
  INNER JOIN
    a
  ON
    timeupdate.video_id=a.video_id),

  ios AS (
  SELECT
    user_id,
    'ios' AS source,
    timestamp,
    content,
    timecode
  FROM
    ios.timeupdate
  INNER JOIN
    a
  ON
    SAFE_CAST(timeupdate.video_id AS int64)=SAFE_CAST(a.video_id AS int64)),

  b AS (
  SELECT user_id,source,timestamp,content,safe_cast(safe_cast(timecode as string) as int64) as timecode FROM web
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
  sum(case when content = 'Heartland' then timecode else 0 end) as heartland_duration,
  sum(case when content = 'Bringing Up Bates' then timecode else 0 end) as bates_duration,
  sum(case when content = 'Other' then timecode else 0 end) as other_duration,
  sum(case when content = 'Heartland' and date_diff(date(timestamp), date(customer_created_at), day)<4 then timecode else 0 end) as heartland_duration_day_1,
  sum(case when content = 'Heartland' and date_diff(date(timestamp), date(customer_created_at), day)>=4 and date_diff(date(timestamp), date(customer_created_at), day)<8 then timecode else 0 end) as heartland_duration_day_2,
  sum(case when content = 'Heartland' and date_diff(date(timestamp), date(customer_created_at), day)>=8 and date_diff(date(timestamp), date(customer_created_at), day)<12 then timecode else 0 end) as heartland_duration_day_3,
  sum(case when content = 'Heartland' and date_diff(date(timestamp), date(customer_created_at), day)>=12 and date_diff(date(timestamp), date(customer_created_at), day)<16 then timecode else 0 end) as heartland_duration_day_4,
  sum(case when content = 'Bringing Up Bates' and date_diff(date(timestamp), date(customer_created_at), day)<4 then timecode else 0 end) as bates_duration_day_1,
  sum(case when content = 'Bringing Up Bates' and date_diff(date(timestamp), date(customer_created_at), day)>=4 and date_diff(date(timestamp), date(customer_created_at), day)<8 then timecode else 0 end) as bates_duration_day_2,
  sum(case when content = 'Bringing Up Bates' and date_diff(date(timestamp), date(customer_created_at), day)>=8 and date_diff(date(timestamp), date(customer_created_at), day)<12 then timecode else 0 end) as bates_duration_day_3,
  sum(case when content = 'Bringing Up Bates' and date_diff(date(timestamp), date(customer_created_at), day)>=12 and date_diff(date(timestamp), date(customer_created_at), day)<16 then timecode else 0 end) as bates_duration_day_4,
  sum(case when content = 'Other' and date_diff(date(timestamp), date(customer_created_at), day)<4 then timecode else 0 end) as other_duration_day_1,
  sum(case when content = 'Other' and date_diff(date(timestamp), date(customer_created_at), day)>=4 and date_diff(date(timestamp), date(customer_created_at), day)<8 then timecode else 0 end) as other_duration_day_2,
  sum(case when content = 'Other' and date_diff(date(timestamp), date(customer_created_at), day)>=8 and date_diff(date(timestamp), date(customer_created_at), day)<12 then timecode else 0 end) as other_duration_day_3,
  sum(case when content = 'Other' and date_diff(date(timestamp), date(customer_created_at), day)>=12 and date_diff(date(timestamp), date(customer_created_at), day)<16 then timecode else 0 end) as other_duration_day_4
FROM
  b LEFT JOIN customers.subscribers ON SAFE_CAST(user_id AS int64)=SAFE_CAST(customer_id AS int64)
where date(timestamp)>=date(customer_created_at) and date(timestamp)<=date_add(date(customer_created_at), interval 14 day)
group by 1,2,3,4,5
order by user_id),

d as
(select customer_id as user_id,
       a.platform,
       a.frequency,
       case when a.campaign is not null then a.campaign else 'unavailable' end as campaign,
       a.customer_created_at,
       case when heartland_duration is null then 0 else heartland_duration end as heartland_duration,
       case when bates_duration is null then 0 else bates_duration end as bates_duration,
       case when other_duration is null then 0 else other_duration end as other_duration,
       case when heartland_duration_day_1 is null then 0 else heartland_duration_day_1 end as heartland_duration_day_1,
       case when heartland_duration_day_2 is null then 0 else heartland_duration_day_2 end as heartland_duration_day_2,
       case when heartland_duration_day_3 is null then 0 else heartland_duration_day_3 end as heartland_duration_day_3,
       case when heartland_duration_day_4 is null then 0 else heartland_duration_day_4 end as heartland_duration_day_4,
       case when bates_duration_day_1 is null then 0 else bates_duration_day_1 end as bates_duration_day_1,
       case when bates_duration_day_2 is null then 0 else bates_duration_day_2 end as bates_duration_day_2,
       case when bates_duration_day_3 is null then 0 else bates_duration_day_3 end as bates_duration_day_3,
       case when bates_duration_day_4 is null then 0 else bates_duration_day_4 end as bates_duration_day_4,
       case when other_duration_day_1 is null then 0 else other_duration_day_1 end as other_duration_day_1,
       case when other_duration_day_2 is null then 0 else other_duration_day_2 end as other_duration_day_2,
       case when other_duration_day_3 is null then 0 else other_duration_day_3 end as other_duration_day_3,
       case when other_duration_day_4 is null then 0 else other_duration_day_4 end as other_duration_day_4
from customers.subscribers as a left join c on customer_id=safe_cast(user_id as int64)),

e as
(select
       avg(heartland_duration) hl_avg, stddev(heartland_duration) as hl_std,
       avg(bates_duration) b_avg, stddev(bates_duration) as b_std,
       avg(other_duration) o_avg, stddev(other_duration) as o_std,
       avg(heartland_duration_day_1) hl1_avg, stddev(heartland_duration_day_1) as hl1_std,
       avg(heartland_duration_day_2) hl2_avg, stddev(heartland_duration_day_2) as hl2_std,
       avg(heartland_duration_day_3) hl3_avg, stddev(heartland_duration_day_3) as hl3_std,
       avg(heartland_duration_day_4) hl4_avg, stddev(heartland_duration_day_4) as hl4_std,
       avg(bates_duration_day_1) b1_avg, stddev(bates_duration_day_1) as b1_std,
       avg(bates_duration_day_2) b2_avg, stddev(bates_duration_day_2) as b2_std,
       avg(bates_duration_day_3) b3_avg, stddev(bates_duration_day_3) as b3_std,
       avg(bates_duration_day_4) b4_avg, stddev(bates_duration_day_4) as b4_std,
       avg(other_duration_day_1) o1_avg, stddev(other_duration_day_1) as o1_std,
       avg(other_duration_day_2) o2_avg, stddev(other_duration_day_2) as o2_std,
       avg(other_duration_day_3) o3_avg, stddev(other_duration_day_3) as o3_std,
       avg(other_duration_day_4) o4_avg, stddev(other_duration_day_4) as o4_std
       from d)

select user_id,
        platform,
        frequency,
        campaign,
        (heartland_duration - hl_avg)/hl_std as heartland_duration,
        (bates_duration - b_avg)/b_std as bates_duration,
        (other_duration - o_avg)/o_std as other_duration,
        (heartland_duration_day_1 - hl1_avg)/hl1_std as heartland_duration_day_1,
        (heartland_duration_day_2 - hl2_avg)/hl2_std as heartland_duration_day_2,
        (heartland_duration_day_3 - hl3_avg)/hl3_std as heartland_duration_day_3,
        (heartland_duration_day_4 - hl4_avg)/hl4_std as heartland_duration_day_4,
        (bates_duration_day_1 - b1_avg)/b1_std as bates_duration_day_1,
        (bates_duration_day_2 - b2_avg)/b2_std as bates_duration_day_2,
        (bates_duration_day_3 - b3_avg)/b3_std as bates_duration_day_3,
        (bates_duration_day_4 - b4_avg)/b4_std as bates_duration_day_4,
        (other_duration_day_1 - o1_avg)/o1_std as other_duration_day_1,
        (other_duration_day_2 - o2_avg)/o2_std as other_duration_day_2,
        (other_duration_day_3 - o3_avg)/o3_std as other_duration_day_3,
        (other_duration_day_4 - o4_avg)/o4_std as other_duration_day_4
 from d, e


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

  dimension: campaign {
    type: string
    sql: ${TABLE}.campaign ;;
  }

  dimension: heartland_duration {
    type: number
    sql: ${TABLE}.heartland_duration;;
  }

  dimension: heartland_duration_day_1 {
    type: number
    sql: ${TABLE}.heartland_duration_day_1 ;;
  }

  dimension: heartland_duration_day_2 {
    type: number
    sql: ${TABLE}.heartland_duration_day_2 ;;
  }

  dimension: heartland_duration_day_3 {
    type: number
    sql: ${TABLE}.heartland_duration_day_3 ;;
  }

  dimension: heartland_duration_day_4 {
    type: number
    sql: ${TABLE}.heartland_duration_day_4 ;;
  }

  dimension: bates_duration {
    type: number
    sql: ${TABLE}.bates_duration ;;
  }

  dimension: bates_duration_day_1 {
    type: number
    sql: ${TABLE}.bates_duration_day_1 ;;
  }

  dimension: bates_duration_day_2 {
    type: number
    sql: ${TABLE}.bates_duration_day_2 ;;
  }

  dimension: bates_duration_day_3 {
    type: number
    sql: ${TABLE}.bates_duration_day_3 ;;
  }

  dimension: bates_duration_day_4 {
    type: number
    sql: ${TABLE}.bates_duration_day_4 ;;
  }

  dimension: other_duration{
    type: number
    sql: ${TABLE}.other_duration ;;
  }

  dimension: other_duration_day_1 {
    type: number
    sql: ${TABLE}.other_duration_day_1 ;;
  }

  dimension: other_duration_day_2 {
    type: number
    sql: ${TABLE}.other_duration_day_2 ;;
  }

  dimension: other_duration_day_3 {
    type: number
    sql: ${TABLE}.other_duration_day_3 ;;
  }

  dimension: other_duration_day_4 {
    type: number
    sql: ${TABLE}.other_duration_day_4 ;;
  }

  }
