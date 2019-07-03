view: facebook {
  sql_table_name: svod_titles.Facebook ;;

  measure: comments_count {
    type: sum
    sql: ${TABLE}.comments_count ;;
  }

  dimension: date {
    type: string
    sql: ${TABLE}.end_time ;;
  }

  measure: engaged_users {
    type: sum
    sql: ${TABLE}.engaged_users ;;
  }

  measure: impressions {
    type: sum
    sql: ${TABLE}.impressions ;;
  }

  measure: likes_count {
    type: sum
    sql: ${TABLE}.likes_count ;;
  }

  measure: page_likes {
    type: sum
    sql: ${TABLE}.page_likes ;;
  }

  measure: paid_video_views {
    type: sum
    sql: ${TABLE}.paid_video_views ;;
  }

  measure: shares_count {
    type: sum
    sql: ${TABLE}.shares_count ;;
  }

  measure: video_views {
    type: sum
    sql: ${TABLE}.video_views ;;
  }



  measure: count {
    type: count
    drill_fields: []
  }
}
