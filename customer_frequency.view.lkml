view: customer_frequency {
  derived_table: {
    sql: select * from  svod_titles.customer_frequency
      ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: customer_id {
    type: number
    sql: ${TABLE}.customer_id ;;
  }

  dimension: email {
    type: string
    sql: ${TABLE}.email ;;
  }

  dimension: status {
    type: string
    sql: ${TABLE}.status ;;
  }

  dimension: frequency {
    type: string
    sql: ${TABLE}.frequency ;;
  }

  dimension: event_created_at {
    type: date
    datatype: date
    sql: ${TABLE}.event_created_at ;;
  }

  dimension: customer_created_at {
    type: date
    datatype: date
    sql: ${TABLE}.customer_created_at ;;
  }

  measure: user_count {
    type: count_distinct
    sql: ${customer_id} ;;
  }

  set: detail {
    fields: [
      customer_id,
      email,
      status,
      frequency,
      event_created_at,
      customer_created_at
    ]
  }
}
