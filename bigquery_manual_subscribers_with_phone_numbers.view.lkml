view: bigquery_manual_subscribers_with_phone_numbers {
  sql_table_name: customers.subscribers_with_phone_numbers ;;

  dimension: email {
    type: string
    sql: ${TABLE}.string_field_0 ;;
  }

  dimension: fname {
    type: string
    sql: ${TABLE}.string_field_1 ;;
  }

  dimension: lname {
    type: string
    sql: ${TABLE}.string_field_2 ;;
  }

  dimension: phone {
    type: string
    sql: ${TABLE}.string_field_3 ;;
  }

  measure: count {
    type: count
    drill_fields: []
  }
}
