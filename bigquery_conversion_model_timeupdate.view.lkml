view: bigquery_conversion_model_timeupdate {
  derived_table: {
    sql:
with
 a3 as
(select distinct CASE
      WHEN collection LIKE '%Heartland%' THEN 'Heartland'
      WHEN collection LIKE '%Bringing Up Bates%' THEN 'Bringing Up Bates'
      ELSE 'Other'
    END AS content,
                 title,
                 id,
                 duration
 from svod_titles.titles_id_mapping
 where (series is null and upper(collection)=upper(title)) or series is not null),

a4 as
((SELECT
    a3.title,
    user_id,
    date(sent_at) as timestamp,
    a3.duration*60 as duration,
    max(timecode) as timecode,
   'web' AS source
  FROM
    javascript.video_playback_started as a inner join a3 on safe_cast(a.video_id as int64)=a3.id
  WHERE
    user_id IS NOT NULL and safe_cast(user_id as string)!='0'
  GROUP BY 1,2,3,4)

union all

(SELECT
    title,
    user_id,
    date(sent_at) as timestamp,
    a3.duration*60 as duration,
    max(timecode) as timecode,
   'iOS' AS source
  FROM
    ios.video_playback_started as a inner join a3 on safe_cast(a.video_id as int64)=a3.id
  WHERE
    user_id IS NOT NULL and safe_cast(user_id as string)!='0'
  GROUP BY 1,2,3,4)

  union all

  (SELECT
    title,
    user_id,
    date(sent_at) as timestamp,
    a3.duration*60 as duration,
    max(timecode) as timecode,
   'roku' AS source
  FROM
    roku.video_playback_started as a inner join a3 on safe_cast(a.video_id as int64)=a3.id
  WHERE
    user_id IS NOT NULL and safe_cast(user_id as string)!='0'
  GROUP BY 1,2,3,4)

  union all

(SELECT
    title,
    user_id,
    date(sent_at) as timestamp,
    a3.duration*60 as duration,
    max(timecode) as timecode,
   'Android' AS source
  FROM
    android.video_playback_started as a inner join a3 on a.video_id=a3.id
  WHERE
    user_id IS NOT NULL and safe_cast(user_id as string)!='0'
  GROUP BY 1,2,3,4)),

b as
(select a4.*,
        content
from a4 inner join a3 on a4.title=a3.title),


purchase_event as
(select distinct  user_id, created_at, platform
from http_api.purchase_event
where date(created_at)>'2018-10-31') ,


 c as
(SELECT
  a.user_id,
  platform,
  created_at,
--   date_diff(date(timestamp),date(customer_created_at),day) as daydiff,
  sum(case when content = 'Heartland' then timecode else 0 end) as heartland_duration,
  sum(case when content = 'Bringing Up Bates' then timecode else 0 end) as bates_duration,
  sum(case when content = 'Other' then timecode else 0 end) as other_duration,
  sum(case when content = 'Heartland' and date_diff((b.timestamp), date(created_at), day)<4 then timecode else 0 end) as heartland_duration_day_1,
  sum(case when content = 'Heartland' and date_diff((b.timestamp), date(created_at), day)>=4 and date_diff((b.timestamp), date(created_at), day)<8 then timecode else 0 end) as heartland_duration_day_2,
  sum(case when content = 'Heartland' and date_diff((b.timestamp), date(created_at), day)>=8 and date_diff((b.timestamp), date(created_at), day)<12 then timecode else 0 end) as heartland_duration_day_3,
  sum(case when content = 'Heartland' and date_diff((b.timestamp), date(created_at), day)>=12 and date_diff((b.timestamp), date(created_at), day)<16 then timecode else 0 end) as heartland_duration_day_4,
  sum(case when content = 'Bringing Up Bates' and date_diff((b.timestamp), date(created_at), day)<4 then timecode else 0 end) as bates_duration_day_1,
  sum(case when content = 'Bringing Up Bates' and date_diff((b.timestamp), date(created_at), day)>=4 and date_diff((b.timestamp), date(created_at), day)<8 then timecode else 0 end) as bates_duration_day_2,
  sum(case when content = 'Bringing Up Bates' and date_diff((b.timestamp), date(created_at), day)>=8 and date_diff((b.timestamp), date(created_at), day)<12 then timecode else 0 end) as bates_duration_day_3,
  sum(case when content = 'Bringing Up Bates' and date_diff((b.timestamp), date(created_at), day)>=12 and date_diff((b.timestamp), date(created_at), day)<16 then timecode else 0 end) as bates_duration_day_4,
  sum(case when content = 'Other' and date_diff((b.timestamp), date(created_at), day)<4 then timecode else 0 end) as other_duration_day_1,
  sum(case when content = 'Other' and date_diff((b.timestamp), date(created_at), day)>=4 and date_diff((b.timestamp), date(created_at), day)<8 then timecode else 0 end) as other_duration_day_2,
  sum(case when content = 'Other' and date_diff((b.timestamp), date(created_at), day)>=8 and date_diff((b.timestamp), date(created_at), day)<12 then timecode else 0 end) as other_duration_day_3,
  sum(case when content = 'Other' and date_diff((b.timestamp), date(created_at), day)>=12 and date_diff((b.timestamp), date(created_at), day)<16 then timecode else 0 end) as other_duration_day_4
FROM
  b right JOIN purchase_event as a ON a.user_id=b.user_id
where (b.timestamp)>=date(created_at) and (b.timestamp)<=date_add(date(created_at), interval 14 day)
group by 1,2,3),

d as
(select a.user_id,
       a.platform,
       a.created_at,
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
from purchase_event as a left join c on a.user_id=c.user_id
where a.user_id<>'0'),

e as
(select
       max(heartland_duration) hl_max, min(heartland_duration) as hl_min,
       max(bates_duration) b_max, min(bates_duration) as b_min,
       max(other_duration) o_max, min(other_duration) as o_min,
       max(heartland_duration_day_1) hl1_max, min(heartland_duration_day_1) as hl1_min,
       max(heartland_duration_day_2) hl2_max, min(heartland_duration_day_2) as hl2_min,
       max(heartland_duration_day_3) hl3_max, min(heartland_duration_day_3) as hl3_min,
       max(heartland_duration_day_4) hl4_max, min(heartland_duration_day_4) as hl4_min,
       max(bates_duration_day_1) b1_max, min(bates_duration_day_1) as b1_min,
       max(bates_duration_day_2) b2_max, min(bates_duration_day_2) as b2_min,
       max(bates_duration_day_3) b3_max, min(bates_duration_day_3) as b3_min,
       max(bates_duration_day_4) b4_max, min(bates_duration_day_4) as b4_min,
       max(other_duration_day_1) o1_max, min(other_duration_day_1) as o1_min,
       max(other_duration_day_2) o2_max, min(other_duration_day_2) as o2_min,
       max(other_duration_day_3) o3_max, min(other_duration_day_3) as o3_min,
       max(other_duration_day_4) o4_max, min(other_duration_day_4) as o4_min
       from d)


select user_id,
        platform,
        created_at as customer_created_at,
        (heartland_duration - hl_min)/(hl_max-hl_min) as heartland_duration,
        (bates_duration - b_min)/(b_max-b_min) as bates_duration,
        (other_duration - o_min)/(o_max-o_min) as other_duration,
        (heartland_duration_day_1 - hl1_min)/(hl1_max-hl1_min) as heartland_duration_day_1,
        (heartland_duration_day_2 - hl2_min)/(hl2_max-hl2_min) as heartland_duration_day_2,
        (heartland_duration_day_3 - hl3_min)/(hl3_max-hl3_min) as heartland_duration_day_3,
        (heartland_duration_day_4 - hl4_min)/(hl4_max-hl4_min) as heartland_duration_day_4,
        (bates_duration_day_1 - b1_min)/(b1_max-b1_min) as bates_duration_day_1,
        (bates_duration_day_2 - b2_min)/(b2_max-b2_min) as bates_duration_day_2,
        (bates_duration_day_3 - b3_min)/(b3_max-b3_min) as bates_duration_day_3,
        (bates_duration_day_4 - b4_min)/(b4_max-b4_min) as bates_duration_day_4,
        (other_duration_day_1 - o1_min)/(o1_max-o1_min) as other_duration_day_1,
        (other_duration_day_2 - o2_min)/(o2_max-o2_min) as other_duration_day_2,
        (other_duration_day_3 - o3_min)/(o3_max-o3_min) as other_duration_day_3,
        (other_duration_day_4 - o4_min)/(o4_max-o4_min) as other_duration_day_4
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
