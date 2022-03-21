view: redshift_customers_views_by_user {
  derived_table: {
    sql: SELECT * FROM customers.views_by_users
      ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: user_id {
    type: number
    sql: ${TABLE}.user_id ;;
  }

  dimension: email {
    type: string
    sql: ${TABLE}.email ;;
  }

  dimension: video_id {
    type: number
    sql: ${TABLE}.video_id ;;
  }

  dimension: title {
    type: string
    sql: ${TABLE}.title ;;
  }

  dimension: platform {
    type: string
    sql: ${TABLE}.platform ;;
  }

  dimension_group: start_date {
    type: time
    sql: ${TABLE}.start_date ;;
  }

  dimension: total_minutes_watched {
    type: string
    sql: ${TABLE}.total_minutes_watched ;;
  }

  set: detail {
    fields: [
      user_id,
      email,
      video_id,
      title,
      platform,
      total_minutes_watched
    ]
  }
}
