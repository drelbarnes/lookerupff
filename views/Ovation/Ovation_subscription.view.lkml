view: ovation_subscription {
  dimension: customer_id {
    type: string
    sql: ${TABLE}.customer_id ;;
  }

  dimension: subscription_id {
    type: string
    sql: ${TABLE}.subscription_id ;;
  }

  dimension: email {
    type: string
    sql: ${TABLE}.email ;;
  }

  dimension: trial_start_date {
    type: date
    sql: ${TABLE}.trial_start_date ;;
  }

  dimension: active_start_date {
    type: date
    sql: ${TABLE}.active_start_date ;;
  }

  dimension: active_end_date {
    type: date
    sql: ${TABLE}.active_end_date ;;
  }

  dimension: product_name {
    type: string
    sql: ${TABLE}.product_name ;;
  }

  dimension: is_involuntary {
    type: yesno
    sql: ${TABLE}.is_involuntary ;;
  }

  dimension: channel {
    type: string
    sql: ${TABLE}.channel ;;
  }

  dimension: is_reconnect {
    type: yesno
    sql: ${TABLE}.is_reconnect ;;
  }

  dimension: subscription_unit {
    type: string
    sql: ${TABLE}.subscription_unit ;;
  }

  dimension: unit_price {
    type: number
    sql: ${TABLE}.unit_price ;;
  }

  dimension: special_offer {
    type: yesno
    sql: ${TABLE}.special_offer ;;
  }

  dimension: unit_price_currency {
    type: string
    sql: ${TABLE}.unit_price_currency ;;
  }
}
