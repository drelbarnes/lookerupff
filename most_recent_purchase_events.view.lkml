view: most_recent_purchase_events {
  derived_table: {
    datagroup_trigger: purchase_event_datagroup
    sql: with purchase_events as (
      SELECT user_id
      , email
      , first_name
      , last_name
      , platform
      , topic
      , subscription_frequency
      , subscription_status
      , moptin as subscriber_marketing_opt_in
      , status_date
      FROM `up-faith-and-family-216419.http_api.purchase_event`
      WHERE status_date >= ((TIMESTAMP_ADD(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), MINUTE), INTERVAL -180 MINUTE)))
      and topic not in ("customer.created")
      )
      , filter as (
      SELECT *
      , ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY status_date DESC) as n
      FROM purchase_events
      )
      -- only returns rows that have the most recent topic, as flagged by the filter table.
      SELECT * FROM filter WHERE n = 1;;
      # sql_trigger_value: SELECT EXTRACT(HOUR FROM CURRENT_TIMESTAMP()) ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: user_id {
    type: string
    tags: ["user_id"]
    primary_key: yes
    sql: ${TABLE}.user_id ;;
  }

  dimension: email {
    type: string
    tags: ["email"]
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

  dimension: topic {
    type: string
    sql: ${TABLE}.topic ;;
  }

  dimension: platform {
    type: string
    sql: ${TABLE}.platform ;;
  }

  # dimension: plan {
  #   type: string
  #   sql: ${TABLE}.plan ;;
  # }

  # dimension: referrer {
  #   type: string
  #   sql: ${TABLE}.referrer ;;
  # }

  dimension: subscription_frequency {
    type: string
    sql: ${TABLE}.subscription_frequency ;;
  }

  dimension: subscription_status {
    type: string
    sql: ${TABLE}.subscription_status ;;
  }

  dimension: subscriber_marketing_opt_in {
    type: yesno
    sql:  ${TABLE}.subscriber_marketing_opt_in ;;
  }

  dimension_group: status_date {
    type: time
    timeframes: [
      raw,
      time,
      date,
      week,
      month,
      quarter,
      year
    ]
    sql: ${TABLE}.status_date ;;
  }

  set: detail {
    fields: [
      topic,
      user_id,
      email,
      first_name,
      last_name,
      platform,
      subscription_frequency,
      subscription_status,
      subscriber_marketing_opt_in
    ]
  }
}
