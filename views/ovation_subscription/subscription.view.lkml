view: subscriptions {
  sql_table_name: your_table_name_here ;;

  dimension: customer_id {
    sql: ${TABLE}.customer_id ;;
  }

  dimension: subscription_id {
    sql: ${TABLE}.subscription_id ;;
  }

  dimension: email {
    sql: ${TABLE}.email ;;
  }

  dimension: trial_start_date {
    sql: ${TABLE}.trial_start_date ;;
    type: date
  }

  dimension: active_start_date {
    sql: ${TABLE}.active_start_date ;;
    type: date
  }

  dimension: active_end_date {
    sql: ${TABLE}.active_end_date ;;
    type: date
  }

  dimension: product_name {
    sql: ${TABLE}.product_name ;;
  }

  dimension: unit_price {
    sql: ${TABLE}.unit_price ;;
    type: number
  }

  dimension: subscription_unit {
    sql: ${TABLE}.subscription_unit ;;
  }

  dimension: channel {
    sql: ${TABLE}.channel ;;
  }

  dimension: special_offer {
    sql: ${TABLE}.special_offer ;;
  }

  dimension: unit_price_currency {
    sql: ${TABLE}.unit_price_currency ;;
  }

  dimension: is_involuntary {
    sql: ${TABLE}.is_involuntary ;;
    type: yesno
  }

  dimension: is_reconnect {
    sql: ${TABLE}.is_reconnect ;;
    type: yesno
  }
}
