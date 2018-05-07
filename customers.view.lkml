view: customers {
  sql_table_name: customers.customers ;;

  dimension: customer_id {
    primary_key: yes
    tags: ["user_id"]
    type: number
    sql: ${TABLE}.customer_id ;;
  }

  dimension: action {
    type: string
    sql: ${TABLE}.action ;;
  }

  dimension: action_type {
    type: string
    sql: ${TABLE}.action_type ;;
  }

  dimension: campaign {
    type: string
    sql: ${TABLE}.campaign ;;
  }

  dimension: city {
    type: string
    sql: ${TABLE}.city ;;
  }



  dimension: country {
    type: string
    map_layer_name: countries
    sql: ${TABLE}.country ;;
  }

  dimension: coupon_code {
    type: string
    sql: ${TABLE}.coupon_code ;;
  }

  dimension: coupon_code_id {
    type: string
    sql: ${TABLE}.coupon_code_id ;;
  }

  dimension: customer_created_at {
    type: date
    sql: ${TABLE}.customer_created_at ;;
  }

  dimension: email {
    type: string
    tags: ["email"]
    sql: ${TABLE}.email ;;
  }

  dimension: event_created_at {
    type: date
    sql: ${TABLE}.event_created_at ;;
  }

  dimension: current_date{
    type: date
    sql: current_date;;
}


  measure: days_churned {
    type: number
    sql:  DATEDIFF('day', ${event_created_at}::timestamp, ${customer_created_at}::timestamp) ;;

  }

  measure: average_days_by {
    type: average
    sql:  DATEDIFF('day', ${customer_created_at}::timestamp, ${event_created_at}::timestamp) ;;
  }

dimension: days_since_created {
  type: number
  sql:  DATEDIFF('day', ${customer_created_at}::timestamp, ${event_created_at}::timestamp);;
}

  measure: max_days_by {
    type: max
    sql:  DATEDIFF('day', ${customer_created_at}::timestamp, ${event_created_at}::timestamp) ;;
  }
  measure: min_days_by {
    type: min
    sql:  DATEDIFF('day', ${customer_created_at}::timestamp, ${event_created_at}::timestamp) ;;
  }

  measure: average_days_on {
    type: average
    sql:  DATEDIFF('day', ${event_created_at}::timestamp, ${current_date}::timestamp) ;;
  }

  dimension: first_name {
    type: string
    sql: ${TABLE}.first_name ;;
  }

  dimension: frequency {
    type: string
    sql: ${TABLE}.frequency ;;
  }

  dimension: last_name {
    type: string
    sql: ${TABLE}.last_name ;;
  }

  dimension: platform {
    type: string
    sql: ${TABLE}.platform ;;
    drill_fields: [status, platform, customer_id, email, customer_created_at, status]
  }

  dimension: product_id {
    type: string
    sql: ${TABLE}.product_id ;;
  }

  dimension: product_name {
    type: string
    sql: ${TABLE}.product_name ;;
  }

  dimension: promotion_code {
    type: string
    sql: ${TABLE}.promotion_code ;;
  }

  dimension: promotion_id {
    type: number
    sql: ${TABLE}.promotion_id ;;
  }

  dimension: referrer {
    type: string
    sql: ${TABLE}.referrer ;;
  }

  dimension: state {
    type: string
    sql: ${TABLE}.state ;;
  }

  dimension: status {
    type: string
    sql: ${TABLE}.status ;;
  }

  dimension: State_Location {
    map_layer_name:us_states
    sql: ${state} ;;
  }

  measure: count {
    type: count
    drill_fields: [customer_id, product_name, last_name, first_name, email]
  }

  measure: customer_count {
    type: count_distinct
    sql: ${customer_id} ;;
  }
}