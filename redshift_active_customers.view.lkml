view: redshift_active_customers {
  derived_table: {
    sql: SELECT * FROM customers.active_customers
      ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: email {
    type: string
    sql: ${TABLE}.email ;;
  }

  dimension: anonymous_id {
    type: string
    tags: ["anonymous_id"]
    sql: "abc-123" ;;
  }

  dimension: user_id {
    type: number
    primary_key: yes
    sql: ${TABLE}.user_id ;;
  }

  dimension: customer_video_notifications {
    type: number
    sql: ${TABLE}.customer_video_notifications ;;
  }

  dimension: subscriptions_in_free_trial {
    type: string
    sql: ${TABLE}.subscriptions_in_free_trial ;;
  }

  dimension: tickets_is_subscriptions {
    type: string
    sql: ${TABLE}.tickets_is_subscriptions ;;
  }

  dimension: ticket_status {
    type: string
    sql: ${TABLE}.ticket_status ;;
  }

  dimension: tickets_subscription_frequency {
    type: string
    sql: ${TABLE}.tickets_subscription_frequency ;;
  }

  dimension_group: report_date {
    type: time
    sql: ${TABLE}.report_date ;;
  }

  measure: discount_count {
    type: count_distinct
    sql: ${user_id} ;;
  }

  measure: free_trials {
    type: count_distinct
    sql: ${user_id} ;;
    filters: [subscriptions_in_free_trial: "Yes"]
  }

  measure: subscribers {
    type: count_distinct
    sql: ${user_id} ;;
    filters: [subscriptions_in_free_trial: "No"]
  }

  set: detail {
    fields: [
      email,
      user_id,
      customer_video_notifications,
      subscriptions_in_free_trial,
      tickets_is_subscriptions,
      ticket_status,
      tickets_subscription_frequency,
      report_date_time
    ]
  }
}
