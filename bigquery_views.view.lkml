view: bigquery_views {
  sql_table_name: customers.views ;;

  dimension: user_id {
    type: number
    sql: ${TABLE}.user_id ;;
  }

  dimension: views_count {
    type: number
    sql: ${TABLE}.views_count ;;
  }

  measure: count {
    type: count
    drill_fields: []
  }
}
