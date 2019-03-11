view: bigquery_timeupdate {
  derived_table: {
    sql:(with a1 as
(select sent_at,
        user_id,
        (split(title," - ")) as title,
        a.current_time as _current_time
from javascript.timeupdate as a),

a2 as
(select sent_at,
        user_id,
        title[safe_ordinal(1)] as title,
        concat(title[safe_ordinal(2)]," - ",title[safe_ordinal(3)]) as collection,
        _current_time
 from a1),

 a3 as
(select *
 from svod_titles.titles_id_mapping
 where (series is null and upper(collection)=upper(title)) or series is not null),

a4 as
((SELECT
    a2.title,
    user_id,
    id as video_id,
    case when a3.collection in ('Season 1','Season 2','Season 3') then concat(series,' ',a3.collection) else a3.collection end as collection,
    series,
    season,
    episode,
    case when series is null and upper(a3.collection)=upper(a3.title) then 'movie'
                     when series is not null then 'series' else 'other' end as type,
    safe_cast(date(sent_at) as timestamp) as timestamp,
    a3.duration*60 as duration,
    max(_current_time) as timecode,
   'web' AS source
  FROM
    a2 inner join a3 on trim(upper(a2.title))=trim(upper(a3.title)) and a2.collection=a3.collection
  WHERE
    user_id IS NOT NULL and safe_cast(user_id as string)!='0' and a3.duration>0
  GROUP BY 1,2,3,4,5,6,7,8,9,10)

union all

(SELECT
    title,
    user_id,
    cast(video_id as int64) as video_id,
    case when collection in ('Season 1','Season 2','Season 3') then concat(series,' ',collection) else collection end as collection,
    series,
    season,
    episode,
    case when series is null and upper(collection)=upper(title) then 'movie'
                     when series is not null then 'series' else 'other' end as type,
    safe_cast(date(sent_at) as timestamp) as timestamp,
    a3.duration*60 as duration,
    max(timecode) as timecode,
   'iOS' AS source
  FROM
    ios.timeupdate as a inner join a3 on safe_cast(a.video_id as int64)=a3.id
  WHERE
    user_id IS NOT NULL and safe_cast(user_id as string)!='0' and a3.duration>0
  GROUP BY 1,2,3,4,5,6,7,8,9,10)

  union all

(SELECT
    title,
    user_id,
    video_id,
    case when collection in ('Season 1','Season 2','Season 3') then concat(series,' ',collection) else collection end as collection,
    series,
    season,
    episode,
    case when series is null and upper(collection)=upper(title) then 'movie'
                     when series is not null then 'series' else 'other' end as type,
    safe_cast(date(sent_at) as timestamp) as timestamp,
    a3.duration*60 as duration,
    max(timecode) as timecode,
   'Android' AS source
  FROM
    android.timeupdate as a inner join a3 on a.video_id=a3.id
  WHERE
    user_id IS NOT NULL and safe_cast(user_id as string)!='0' and a3.duration>0
  GROUP BY 1,2,3,4,5,6,7,8,9,10)

  union all

  (SELECT
    a3.title,
    a.user_id,
     mysql_roku_firstplays_video_id as video_id,
    case when collection in ('Season 1','Season 2','Season 3') then concat(series,' ',collection) else collection end as collection,
    series,
    season,
    episode,
    case when series is null and upper(collection)=upper(title) then 'movie'
                     when series is not null then 'series' else 'other' end as type,
    mysql_roku_firstplays_firstplay_date_date  as timestamp,
    a3.duration*60 as duration,
    mysql_roku_firstplays_total_minutes_watched*60 as timecode,
   'Roku' AS source
  FROM
    looker.roku_firstplays as a inner join a3 on  mysql_roku_firstplays_video_id=a3.id
  WHERE
    user_id IS NOT NULL and user_id<>'0' and a3.duration>0 and date(sent_at)=current_date()))

  select *,
       case when date(a.timestamp) between DATE_SUB(date(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), QUARTER)), INTERVAL 0 QUARTER) and
            DATE_SUB(date(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY)), INTERVAL 0 QUARTER) then "Current Quarter"
            when date(a.timestamp) between DATE_SUB(date(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), QUARTER)), INTERVAL 1 QUARTER) and
            DATE_SUB(date(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY)), INTERVAL 1 QUARTER) then "Prior Quarter"
            when date(a.timestamp) between DATE_SUB(date(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), QUARTER)), INTERVAL 4 QUARTER) and
            DATE_SUB(date(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY)), INTERVAL 4 QUARTER) then "YAGO Quarter"
            else "NA"
            end as Quarter
from a4 as a);;
  }

  dimension: Quarter {
    type: string
    sql: ${TABLE}.quarter ;;
  }

  dimension: current_date {
    type: date
    sql: current_date() ;;
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
    sql: 1.0*${hours_count}/${user_count} ;;
    value_format: "0.0"
  }

  measure: minutes_watched_per_user {
    type: number
    sql: 1.00*${minutes_count}/${user_count} ;;
    value_format: "0"
  }

## filter determining time range for all "A" measures
  filter: time_a {
    type: date_time
  }

## flag for "A" measures to only include appropriate time range
  dimension: group_a {
    hidden: no
    type: yesno
    sql: {% condition time_a %} ${timestamp_raw} {% endcondition %}
      ;;
  }

  measure: hours_a {
    type: sum
    filters: {
      field: group_a
      value: "yes"
    }
    sql: ${hours_watched} ;;
    value_format: "#,##0"
  }

## filter determining time range for all "b" measures
  filter: time_b {
    type: date_time
  }

## flag for "B" measures to only include appropriate time range
  dimension: group_b {
    hidden: no
    type: yesno
    sql: {% condition time_b %} ${timestamp_raw} {% endcondition %}
      ;;
  }

  measure: hours_b {
    type: sum
    filters: {
      field: group_b
      value: "yes"
    }
    sql: ${hours_watched} ;;
    value_format: "#,##0"
  }

  measure: user_count_a {
    type: count_distinct
    filters: {
      field: group_a
      value: "yes"
    }
    sql: ${user_id}  ;;
    value_format: "#,##0"
  }

  measure: user_count_b {
    type: count_distinct
    filters: {
      field: group_b
      value: "yes"
    }
    sql: ${user_id} ;;
    value_format: "#,##0"
  }


# ----- Sets of fields for drilling ------
  set: detail {
    fields: [
      platform,
      user_id
    ]
  }
}
