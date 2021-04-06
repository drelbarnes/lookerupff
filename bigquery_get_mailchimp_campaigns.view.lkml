view: bigquery_get_mailchimp_campaigns {
  sql_table_name: looker.get_mailchimp_campaigns ;;
  drill_fields: [id]

  dimension: id {
    primary_key: yes
    type: string
    sql: ${TABLE}.id ;;
  }

  dimension: anonymous_id {
    type: string
    sql: ${TABLE}.anonymous_id ;;
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

  dimension: email {
    type: string
    sql: ${TABLE}.email ;;
  }

  dimension: event {
    type: string
    sql: ${TABLE}.event ;;
  }

  dimension: event_text {
    type: string
    sql: ${TABLE}.event_text ;;
  }

  dimension: mysql_email_campaigns_action {
    type: string
    sql: ${TABLE}.mysql_email_campaigns_action ;;
  }

  measure: open_count {
    type: count_distinct
    sql: case when ${mysql_email_campaigns_action}='open' then ${email} end ;;
  }

  measure: click_count {
    type: sum
    sql: case when ${mysql_email_campaigns_action}='click' then 1 else 0 end ;;
  }

  measure: email_count {
    type: count_distinct
    sql: ${email} ;;
  }

  dimension: mysql_email_campaigns_campaign_id {
    type: string
    sql: ${TABLE}.mysql_email_campaigns_campaign_id ;;
  }

  dimension: mysql_email_campaigns_id {
    type: number
    sql: ${TABLE}.mysql_email_campaigns_id ;;
  }

  dimension: mysql_email_campaigns_ip {
    type: string
    sql: ${TABLE}.mysql_email_campaigns_ip ;;
  }

  dimension_group: mysql_email_campaigns_timestamp {
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
    sql: ${TABLE}.mysql_email_campaigns_timestamp_date ;;
  }

  dimension: mysql_email_campaigns_url {
    type: string
    sql: ${TABLE}.mysql_email_campaigns_url ;;
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

  dimension: userid {
    type: number
    value_format_name: id
    sql: ${TABLE}.userid ;;
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
    drill_fields: [id, context_app_name, context_library_name]
  }


  measure: user_count {
    type: count_distinct
    sql: ${email} ;;
  }
}
