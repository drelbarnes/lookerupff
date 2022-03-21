view: redshift_customers_resubscribers {
  derived_table: {
    sql: select * FROM customers.resubscribers
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

  dimension: latest_ticket_status {
    type: string
    sql: ${TABLE}.latest_ticket_status ;;
  }

  dimension: total_ticket_count {
    type: number
    sql: ${TABLE}.total_ticket_count ;;
  }

  dimension: latest_ticket_id {
    type: number
    sql: ${TABLE}.latest_ticket_id ;;
  }

  dimension: latest_ticket_created {
    type: string
    sql: ${TABLE}.latest_ticket_created ;;
  }

  set: detail {
    fields: [
      user_id,
      email,
      latest_ticket_status,
      total_ticket_count,
      latest_ticket_id,
      latest_ticket_created
    ]
  }
}
