view: bigquery_timeupdate {
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
 from a1 order by 1),

 a3 as
(select *
 from svod_titles.titles_id_mapping
 where (series is null and upper(collection)=upper(title)) or series is not null),

a4 as
((SELECT
    a2.title,
    user_id,
    safe_cast(date(sent_at) as timestamp) as timestamp,
    a3.duration*60 as duration,
    max(_current_time) as timecode,
   'web' AS source
  FROM
    a2 inner join a3 on trim(upper(a2.title))=trim(upper(a3.title))
  WHERE
    user_id IS NOT NULL and safe_cast(user_id as string)!='0'
  GROUP BY 1,2,3,4)

union all

(SELECT
    title,
    user_id,
    safe_cast(date(sent_at) as timestamp) as timestamp,
    a3.duration*60 as duration,
    max(timecode) as timecode,
   'iOS' AS source
  FROM
    ios.timeupdate as a inner join a3 on safe_cast(a.video_id as int64)=a3.id
  WHERE
    user_id IS NOT NULL and safe_cast(user_id as string)!='0'
  GROUP BY 1,2,3,4)

  union all

(SELECT
    title,
    user_id,
    safe_cast(date(sent_at) as timestamp) as timestamp,
    a3.duration*60 as duration,
    max(timecode) as timecode,
   'Android' AS source
  FROM
    android.timeupdate as a inner join a3 on a.video_id=a3.id
  WHERE
    user_id IS NOT NULL and safe_cast(user_id as string)!='0'
  GROUP BY 1,2,3,4))

  select a4.*,
         collection,
         case when series is null and upper(collection)=upper(a3.title) then 'movie'
                     when series is not null then 'series' else 'other' end as type
  from a4 inner join a3 on a4.title=a3.title;;
  }

  dimension: title {
    type: string
    sql: ${TABLE}.title;;
  }

  dimension: type {
    type: string
    sql: ${TABLE}.type ;;
  }

  dimension: series {
    type: string
    sql: ${TABLE}.series ;;
  }

  dimension: season {
    type: string
    sql: ${TABLE}.season ;;
  }

  dimension: episode {
    type: string
    sql: ${TABLE}.episode ;;
  }

  dimension: status {
    type: string
    sql: ${TABLE}.status ;;
  }

  dimension: collection {
    type: string
    sql: ${TABLE}.collection ;;
  }

  dimension: platform {
    type: string
    sql: ${TABLE}.platform ;;
  }

  dimension: source {
    type: string
    sql: ${TABLE}.source ;;
  }


  dimension: timecode {
    type: number
    sql: ${TABLE}.timecode  ;;
  }

  dimension: duration {
    type: number
    sql: ${TABLE}.duration ;;
  }

  dimension: hours_watched {
    type: number
    sql: ${timecode}/3600 ;;
    value_format: "#,##0"
  }

  dimension: minutes_watched {
    type: number
    sql: ${timecode}/60 ;;
    value_format: "#,##0"
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

  measure: count {
    type: count
    drill_fields: [detail*]
  }


  measure: duration_count {
    type: sum
    sql: ${duration} ;;
  }

  measure: percent_completed {
    type: number
    value_format: "0\%"
    sql: 100.00*${timecode_count}/${duration_count} ;;
  }

  measure: timecode_count {
    type: sum
    value_format: "0"
    sql: ${timecode} ;;
  }

  measure: hours_count {
    type: sum
    value_format: "#,##0"
    sql: ${hours_watched};;
  }

  measure: minutes_count {
    type: sum
    value_format: "#,##0"
    sql: ${minutes_watched};;
  }

  measure: user_count {
    type: count_distinct
    sql: ${user_id} ;;
  }

  measure: hours_watched_per_user {
    type: number
    sql: 1.00*${hours_count}/${user_count} ;;
    value_format: "0"
  }

  measure: minutes_watched_per_user {
    type: number
    sql: 1.00*${minutes_count}/${user_count} ;;
    value_format: "0"
  }

# ----- Sets of fields for drilling ------
  set: detail {
    fields: [
      platform,
      user_id
    ]
  }
}
