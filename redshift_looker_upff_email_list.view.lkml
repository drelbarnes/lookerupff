view: redshift_looker_upff_email_list {
  derived_table: {
    sql: SELECT * FROM looker.get_upff_email_list WHERE ((( redshift_php_get_email_campaigns_timestamp_date  ) >= ((DATEADD(week,-4, DATE_TRUNC('week', DATE_TRUNC('day',GETDATE())) ))) AND ( redshift_php_get_email_campaigns_timestamp_date  ) <
((DATEADD(week,5, DATEADD(week,-4, DATE_TRUNC('week', DATE_TRUNC('day',GETDATE())) ) )))))
      ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: id {
    primary_key: yes
    type: string
    sql: ${TABLE}.id ;;
  }

  dimension_group: received_at {
    type: time
    sql: ${TABLE}.received_at ;;
  }

  dimension: uuid {
    type: number
    sql: ${TABLE}.uuid ;;
  }

  dimension: context_app_name {
    type: string
    sql: ${TABLE}.context_app_name ;;
  }

  dimension_group: campaigns_timestamp_date {
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

  dimension: context_library_name {
    type: string
    sql: ${TABLE}.context_library_name ;;
  }

  dimension: event {
    type: string
    sql: ${TABLE}.event ;;
  }

  dimension_group: redshift_php_get_email_campaigns_campaign_send_date {
    type: time
    sql: ${TABLE}.redshift_php_get_email_campaigns_campaign_send_date ;;
  }

  dimension: redshift_php_get_email_campaigns_list_id {
    type: string
    sql: ${TABLE}.redshift_php_get_email_campaigns_list_id ;;
  }

  dimension: redshift_php_get_email_campaigns_list_name {
    type: string
    sql: ${TABLE}.redshift_php_get_email_campaigns_list_name ;;
  }

  dimension: context_app_version {
    type: string
    sql: ${TABLE}.context_app_version ;;
  }

  dimension: context_library_version {
    type: string
    sql: ${TABLE}.context_library_version ;;
  }

  dimension: redshift_php_get_email_campaigns_campaign_id {
    type: string
    sql: ${TABLE}.redshift_php_get_email_campaigns_campaign_id ;;
  }

  dimension: redshift_php_get_email_campaigns_campaign_title {
    type: string
    sql: ${TABLE}.redshift_php_get_email_campaigns_campaign_title ;;
  }

  dimension: redshift_php_get_email_campaigns_open_rate {
    type: number
    sql: ${TABLE}.redshift_php_get_email_campaigns_opens_open_rate ;;
  }

  dimension_group: uuid_ts {
    type: time
    sql: ${TABLE}.uuid_ts ;;
  }

  dimension: event_text {
    type: string
    sql: ${TABLE}.event_text ;;
  }

  dimension_group: original_timestamp {
    type: time
    sql: ${TABLE}.original_timestamp ;;
  }

  dimension_group: sent_at {
    type: time
    sql: ${TABLE}.sent_at ;;
  }



  set: detail {
    fields: [
      id,
      received_at_time,
      uuid,
      context_app_name,
      timestamp_time,
      user_id,
      context_library_name,
      event,
      redshift_php_get_email_campaigns_campaign_send_date_time,
      redshift_php_get_email_campaigns_list_id,
      redshift_php_get_email_campaigns_list_name,
      context_app_version,
      context_library_version,
      redshift_php_get_email_campaigns_campaign_id,
      redshift_php_get_email_campaigns_campaign_title,
      redshift_php_get_email_campaigns_open_rate,
      uuid_ts_time,
      event_text,
      original_timestamp_time,
      sent_at_time
    ]
  }


  measure: list_open_rate {
    type: average
    value_format: "0\%"
    sql: (${redshift_php_get_email_campaigns_open_rate} * 100) ;;
  }


}
