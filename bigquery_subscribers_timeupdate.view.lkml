view: bigquery_subscribers_timeupdate {
  sql_table_name: customers.timeupdate ;;

  dimension: timecode_count {
    type: number
    sql: ${TABLE}.timecode_count ;;
  }

  dimension: user_id {
    type: number
    sql: ${TABLE}.user_id ;;
  }

  measure: count {
    type: count
    drill_fields: []
  }
}
