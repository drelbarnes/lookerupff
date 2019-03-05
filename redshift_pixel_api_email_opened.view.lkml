view: redshift_pixel_api_email_opened {
  sql_table_name: pixel_tracking_api.email_opened ;;

  dimension: id {
    primary_key: yes
    type: string
    sql: ${TABLE}.id ;;
  }

  dimension: context_direct {
    type: yesno
    sql: ${TABLE}.context_direct ;;
  }

  dimension: context_ip {
    type: string
    sql: ${TABLE}.context_ip ;;
  }

  dimension: context_library_name {
    type: string
    sql: ${TABLE}.context_library_name ;;
  }

  dimension: context_library_version {
    type: string
    sql: ${TABLE}.context_library_version ;;
  }

  dimension: email {
    type: string
    sql: ${TABLE}.email ;;
  }

  dimension: email_name {
    type: string
    sql: ${TABLE}.email_name ;;
  }

  dimension: event {
    type: string
    sql: ${TABLE}.event ;;
  }

  dimension: event_text {
    type: string
    sql: ${TABLE}.event_text ;;
  }

  dimension: gift_code {
    type: string
    sql: ${TABLE}.gift_code ;;
  }

  dimension: job_id {
    type: string
    sql: ${TABLE}.job_id ;;
  }

  dimension: original_timestamp {
    type: string
    sql: ${TABLE}.original_timestamp ;;
  }

  dimension_group: received {
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
    sql: ${TABLE}.received_at ;;
  }

  dimension_group: sent {
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
    sql: ${TABLE}.sent_at ;;
  }

  dimension: subject {
    type: string
    sql: ${TABLE}.subject ;;
  }

  dimension_group: timestamp {
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
    sql: ${TABLE}.timestamp ;;
  }

  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
  }

  dimension: uuid {
    type: number
    value_format_name: id
    sql: ${TABLE}.uuid ;;
  }

  dimension_group: uuid_ts {
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
    sql: ${TABLE}.uuid_ts ;;
  }

  measure: count {
    type: count
    drill_fields: [id, context_library_name, email_name]
  }

  measure: conversion_rate {
    type: number
    value_format: ".0#\%"
    sql: 100.0*${count_ids}/${http_api_purchase_event.distinct_count};;
  }

  measure: count_ids {
    type: count_distinct
    sql: ${user_id} ;;
  }

  measure: moption_yes_conversion_rate {
    type: number
    value_format: ".0#\%"
    sql: 100.0*${count_ids}/${http_api_purchase_event.moptin_yes};;
  }

  measure: email_opened_conversion_rate {
    type: number
    value_format: ".0#\%"
    sql: 100.0*${count_ids}/${http_api_purchase_event.distinct_count};;
  }

}
