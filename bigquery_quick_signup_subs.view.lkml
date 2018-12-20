view: bigquery_quick_signup_subs {
  sql_table_name: customers.quick_signup_subs ;;

  dimension: email_address {
    type: string
    sql: ${TABLE}.Email_Address ;;
  }

  dimension: first_name {
    type: string
    sql: ${TABLE}.First_Name ;;
  }

  dimension: last_name {
    type: string
    sql: ${TABLE}.Last_Name ;;
  }

  dimension: phone {
    type: number
    tags: ["phone"]
    sql: ${TABLE}.Phone ;;
  }

  dimension: user_id {
    type: number
    sql: ${TABLE}.UserID ;;
  }

  measure: count {
    type: count
    drill_fields: [first_name, last_name]
  }
}
