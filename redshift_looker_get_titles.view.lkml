view: redshift_looker_get_titles {
  derived_table: {
    sql: SELECT distinct title, video_id, max(timestamp) as timestamp, is_available FROM php.get_titles GROUP BY 1,2
      ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: title {
    type: string
    sql: ${TABLE}.title ;;
  }

  dimension: video_id {
    type: number
    sql: ${TABLE}.video_id ;;
  }

  dimension_group: max {
    type: time
    sql: ${TABLE}.max ;;
  }

  dimension: is_available {
    type: string
    sql: ${TABLE}.is_available ;;
  }

  set: detail {
    fields: [title, video_id, max_time]
  }
}
