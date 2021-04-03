view: get_upff_email_list {
  sql_table_name: looker.get_upff_email_list ;;
  drill_fields: [id]

  dimension: id {
    primary_key: yes
    type: string
    sql: ${TABLE}.id ;;
  }

  dimension: context_app_name {
    type: string
    sql: ${TABLE}.context_app_name ;;
  }

  dimension: context_app_version {
    type: string
    sql: ${TABLE}.context_app_version ;;
  }

  dimension: context_library_name {
    type: string
    sql: ${TABLE}.context_library_name ;;
  }

  dimension: context_library_version {
    type: string
    sql: ${TABLE}.context_library_version ;;
  }

  dimension: event {
    type: string
    sql: ${TABLE}.event ;;
  }

  dimension: event_text {
    type: string
    sql: ${TABLE}.event_text ;;
  }

  dimension_group: original_timestamp {
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

  dimension: redshift_php_get_email_campaigns_campaign_id {
    type: string
    sql: ${TABLE}.redshift_php_get_email_campaigns_campaign_id ;;
  }

  dimension_group: redshift_php_get_email_campaigns_campaign_send {
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
    sql: ${TABLE}.redshift_php_get_email_campaigns_campaign_send_date ;;
  }

  dimension: redshift_php_get_email_campaigns_campaign_title {
    type: string
    sql: ${TABLE}.redshift_php_get_email_campaigns_campaign_title ;;
  }

  dimension: redshift_php_get_email_campaigns_list_id {
    type: string
    sql: ${TABLE}.redshift_php_get_email_campaigns_list_id ;;
  }

  dimension: redshift_php_get_email_campaigns_list_name {
    type: string
    sql: ${TABLE}.redshift_php_get_email_campaigns_list_name ;;
  }

  dimension: redshift_php_get_email_campaigns_opens_open_rate {
    type: number
    sql: ${TABLE}.redshift_php_get_email_campaigns_opens_open_rate ;;
  }

  dimension_group: redshift_php_get_email_campaigns_timestamp {
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
    sql: ${TABLE}.redshift_php_get_email_campaigns_timestamp_date ;;
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
    drill_fields: [id, context_app_name, context_library_name, redshift_php_get_email_campaigns_list_name]
  }
}
