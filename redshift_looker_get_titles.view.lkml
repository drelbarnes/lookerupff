view: redshift_looker_get_titles {
  derived_table: {
    sql: SELECT distinct title, video_id, max(timestamp) as timestamp, is_available, additional_images_aspect_ratio_1_1_medium FROM php.get_titles GROUP BY 1,2,4,5
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

  dimension_group: timestamp {
    type: time
    sql: ${TABLE}.timestamp ;;
  }

  dimension: 1_1_medium_image {
    type: string
    sql:  ${TABLE}.additional_images_aspect_ratio_1_1_medium ;;
  }

  dimension: is_available {
    type: yesno
    sql: ${TABLE}.is_available ;;
  }

  set: detail {
    fields: [title, video_id, timestamp_time]
  }
}
