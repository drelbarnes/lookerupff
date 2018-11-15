view: bigquery_churn_model_timeupdate {
  derived_table: {
    sql: with a1 as
(select sent_at,
        user_id,
        (split(title," - ")) as title,
        a.current_time as _current_time
from javascript.timeupdate as a),

a2 as
(select sent_at,
        user_id,
        title[safe_ordinal(1)] as title,
        _current_time
 from a1),

 a3 as
(select *
 from svod_titles.titles_id_mapping
 where (series is null and upper(collection)=upper(title)) or series is not null),

a4 as
((SELECT
    a2.title,
    safe_cast(id as string) as video_id,
    user_id,
    date(sent_at) as timestamp,
    a3.duration*60 as duration,
    max(_current_time) as timecode,
   'web' AS source
  FROM
    a2 inner join a3 on trim(upper(a2.title))=trim(upper(a3.title))
  WHERE
    user_id IS NOT NULL and safe_cast(user_id as string)!='0'
  GROUP BY 1,2,3,4,5)

union all

(SELECT
    title,
    video_id,
    user_id,
    date(sent_at) as timestamp,
    a3.duration*60 as duration,
    max(timecode) as timecode,
   'iOS' AS source
  FROM
    ios.timeupdate as a inner join a3 on safe_cast(a.video_id as int64)=a3.id
  WHERE
    user_id IS NOT NULL and safe_cast(user_id as string)!='0'
  GROUP BY 1,2,3,4,5)

  union all

(SELECT
    title,
    safe_cast(video_id as string) as video_id,
    user_id,
    date(sent_at) as timestamp,
    a3.duration*60 as duration,
    max(timecode) as timecode,
   'Android' AS source
  FROM
    android.timeupdate as a inner join a3 on a.video_id=a3.id
  WHERE
    user_id IS NOT NULL and safe_cast(user_id as string)!='0'
  GROUP BY 1,2,3,4,5)),

a5 as
(select a4.*,
         collection,
         case when series is null and upper(collection)=upper(a3.title) then 'movie'
                     when series is not null then 'series' else 'other' end as type
from a4 inner join a3 on a4.title=a3.title),

a as
(select *,
        CASE
      WHEN collection LIKE '%Heartland%' THEN 'Heartland'
      WHEN collection LIKE '%Bringing Up Bates%' THEN 'Bringing Up Bates'
      ELSE 'Other'
    END AS content
from a5)

  SELECT user_id,
         source,
         timestamp,
         case when content="Other" then timecode else 0 end as other_duration,
         case when content="Heartland" then timecode else 0 end as heartland_duration,
         case when content="Bringing Up Bates" then timecode else 0 end as bates_duration
 from a;;
  }

  dimension: user_id {
    primary_key: yes
    tags: ["user_id"]
    type: string
    sql: ${TABLE}.user_id ;;
  }

  dimension_group: timestamp {
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
    sql: ${TABLE}.timestamp ;;
  }

  measure: other_duration {
    type: sum
    sql: ${TABLE}.other_duration ;;
  }

  measure: bates_duration {
    type: sum
    sql: ${TABLE}.bates_duration ;;
  }

  measure: heartland_duration {
    type: sum
    sql: ${TABLE}.heartland_duration ;;
  }
}
