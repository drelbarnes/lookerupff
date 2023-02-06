view: mysql_up_recommendations_lists {

  derived_table: {
    sql: SELECT id, user_id, video_id, timestamp FROM admin_recommendations.lists
      ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: id {
    type: number
    sql: ${TABLE}.id ;;
  }

  dimension: user_id {
    type: number
    primary_key: yes
    tags: ["user_id"]
    sql: ${TABLE}.user_id ;;
  }

  dimension: video_id {
    type: number
    sql: ${TABLE}.video_id ;;
  }

  dimension_group: timestamp {
    type: time
    sql: ${TABLE}.timestamp ;;
  }

  set: detail {
    fields: [id, user_id, video_id, timestamp_time]
  }

}
