view: gender {
  sql_table_name: svod_titles.gender ;;

  dimension: name {
    type: string
    sql: ${TABLE}.string_field_0 ;;
  }

  dimension: gender {
    type: string
    sql: ${TABLE}.string_field_1 ;;
  }

  measure: count {
    type: count
    drill_fields: []
  }
}
