view: bigquery_timeupdate {
  derived_table: {
    sql: with a1 as
(select sent_at,
        user_id,
        (split(title," - ")) as title,
        a.current_time as _current_time,
        duration
from javascript.timeupdate as a),

a2 as
(select sent_at,
        user_id,
        title[safe_ordinal(1)] as title,
        _current_time,
        duration
 from a1 order by 1),

 a3 as
(select * except(duration)
 from svod_titles.titles_id_mapping
 where (series is null and upper(collection)=upper(title)) or series is not null),

b as
(select safe_cast(id as string) as user_id,
       safe_cast(b.id as string) as video_id,
       duration,
       safe_cast(date(sent_at) as timestamp) as timestamp,
       max(a._current_time) as timecode,
       'Web' as source
from a2 as a inner join a3 as b on trim(upper(a.title))=trim(upper(b.title))
where safe_cast(id as string) != '0'
group by 1,2,3,4
union all
select safe_cast(user_id as string) as user_id,
        safe_cast(video_id as string) as video_id,
        duration,
        safe_cast(date(sent_at) as timestamp) as timestamp,
        max(timecode) as timecode,
        'Android' as source
from android.timeupdate
where safe_cast(user_id as string) != '0'
group by 1,2,3,4
union all
select safe_cast(user_id as string) as user_id,
        video_id,
        duration,
        safe_cast(date(sent_at) as timestamp) as timestamp,
        max(timecode) as timecode,
        'iOS' as source
from ios.timeupdate as a
where safe_cast(user_id as string) != '0'
group by 1,2,3,4)

select b.*,
       collection,
       title,
       case when series is null and upper(collection)=upper(title) then 'movie'
                     when series is not null then 'series' else 'other' end as type
from b inner join a3 on video_id=safe_cast(id as string);;
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
    value_format: "0.00"
  }

  dimension: minutes_watched {
    type: number
    sql: ${timecode}/60 ;;
    value_format: "0.00"
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
    value_format: "0.00\%"
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
    value_format: "0.00"
    sql: ${minutes_watched};;
  }

  measure: user_count {
    type: count_distinct
    sql: ${user_id} ;;
  }

  measure: hours_watched_per_user {
    type: number
    sql: 1.00*${hours_count}/${user_count} ;;
    value_format: "0.00"
  }

  measure: minutes_watched_per_user {
    type: number
    sql: 1.00*${minutes_count}/${user_count} ;;
    value_format: "0.00"
  }

# ----- Sets of fields for drilling ------
  set: detail {
    fields: [
      platform,
      user_id
    ]
  }
}
