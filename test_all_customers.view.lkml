
view: test_all_customers {
  derived_table: {
    sql: SELECT * FROM customers.all_customers WHERE date(report_date) = '2024-12-08' ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: user_id {
    type: number
    primary_key: yes
    sql: ${TABLE}.user_id ;;
  }

  dimension: email {
    type: string
    sql: ${TABLE}.email ;;
  }

  dimension: first_name {
    type: string
    sql: ${TABLE}.first_name ;;
  }

  dimension: last_name {
    type: string
    sql: ${TABLE}.last_name ;;
  }

  dimension: city {
    type: string
    sql: ${TABLE}.city ;;
  }

  dimension: state {
    type: string
    sql: ${TABLE}.state ;;
  }

  dimension: country {
    type: string
    sql: ${TABLE}.country ;;
  }

  dimension: product_id {
    type: string
    sql: ${TABLE}.product_id ;;
  }

  dimension: product_name {
    type: string
    sql: ${TABLE}.product_name ;;
  }

  dimension: action {
    type: string
    sql: ${TABLE}.action ;;
  }

  dimension: action_type {
    type: string
    sql: ${TABLE}.action_type ;;
  }

  dimension: status {
    type: string
    sql: ${TABLE}.status ;;
  }

  dimension: frequency {
    type: string
    sql: ${TABLE}.frequency ;;
  }

  dimension: platform {
    type: string
    sql: ${TABLE}.platform ;;
  }

  dimension: coupon_code {
    type: string
    sql: ${TABLE}.coupon_code ;;
  }

  dimension: coupon_code_id {
    type: string
    sql: ${TABLE}.coupon_code_id ;;
  }

  dimension: promotion_id {
    type: string
    sql: ${TABLE}.promotion_id ;;
  }

  dimension: promotion_code {
    type: string
    sql: ${TABLE}.promotion_code ;;
  }

  dimension: campaign {
    type: string
    sql: ${TABLE}.campaign ;;
  }

  dimension: referrer {
    type: string
    sql: ${TABLE}.referrer ;;
  }

  dimension: event_created_at {
    type: string
    sql: ${TABLE}.event_created_at ;;
  }

  dimension: customer_created_at {
    type: string
    sql: ${TABLE}.customer_created_at ;;
  }

  dimension: expiration_date {
    type: string
    sql: ${TABLE}.expiration_date ;;
  }

  dimension: marketing_opt_in {
    type: string
    sql: ${TABLE}.marketing_opt_in ;;
  }

  dimension: report_date {
    type: string
    sql: ${TABLE}.report_date ;;
  }

  dimension: promotion_id_long {
    type: number
    sql: ${TABLE}.promotion_id_long ;;
  }

  set: detail {
    fields: [
      user_id,
      email,
      first_name,
      last_name,
      city,
      state,
      country,
      product_id,
      product_name,
      action,
      action_type,
      status,
      frequency,
      platform,
      coupon_code,
      coupon_code_id,
      promotion_id,
      promotion_code,
      campaign,
      referrer,
      event_created_at,
      customer_created_at,
      expiration_date,
      marketing_opt_in,
      report_date,
      promotion_id_long
    ]
  }
}
