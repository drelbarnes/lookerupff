view: bigquery_conversion_model_timeupdate {
  derived_table: {
    sql:
with
titles_id_mapping as
(select distinct
       metadata_series_name as series,
       case when metadata_season_name in ('Season 1','Season 2','Season 3') then concat(metadata_series_name,'-',metadata_season_name)
            when metadata_season_name is null then metadata_movie_name else metadata_season_name end as collection,
       season_number as season,
       a.title,
       video_id as id,
       episode_number as episode,
       date(time_available) as date,
       round(duration_seconds/60) as duration,
       promotion
from php.get_titles as a inner join svod_titles.titles_id_mapping as b on a.video_id=b.id
where date(ingest_at)>='2020-02-13' ),

 a3 as
(select distinct CASE
      WHEN collection LIKE '%Heartland%' THEN 'Heartland'
      WHEN collection LIKE '%Bringing Up Bates%' THEN 'Bringing Up Bates'
      ELSE 'Other'
    END AS content,
                 title,
                 id,
                 duration
 from titles_id_mapping
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
group by 1,2,3)

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
where a.user_id<>'0')
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
