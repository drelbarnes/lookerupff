view: bigquery_firstplay {
  derived_table: {
    sql: select user_id, timestamp, safe_cast(video_id as string) as title from android.firstplay
      union all
      select user_id, timestamp, video_id as title from ios.firstplay
      union all
      select user_id, timestamp, title from javascript.firstplay
       ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
  }

  dimension: title {
    type: string
    sql: ${TABLE}.title ;;
  }

  dimension_group: timestamp {
    type: time
    sql: ${TABLE}.timestamp ;;
  }

  measure: user_count {
    type: count_distinct
    sql: ${user_id} ;;
  }

  measure: play_count {
    type: count_distinct
    sql: concat(${user_id},${title},cast(${timestamp_date} as string)) ;;
  }

  measure: views_per_user {
    type: number
    sql: 1.0*${play_count}/${user_count} ;;
    value_format: "0.0"
  }


  set: detail {
    fields: [user_id, timestamp_time]
  }
}
