view: redshift_get_email_automation_emails {
  sql_table_name: php.get_email_automation_emails ;;
  drill_fields: [id]

  dimension: id {
    primary_key: yes
    type: string
    sql: ${TABLE}.id ;;
  }

  dimension: archive_url {
    type: string
    sql: ${TABLE}.archive_url ;;
  }

  dimension: content_type {
    type: string
    sql: ${TABLE}.content_type ;;
  }

  dimension: context_library_consumer {
    type: string
    sql: ${TABLE}.context_library_consumer ;;
  }

  dimension: context_library_name {
    type: string
    sql: ${TABLE}.context_library_name ;;
  }

  dimension: context_library_version {
    type: string
    sql: ${TABLE}.context_library_version ;;
  }

  dimension_group: create {
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
    sql: ${TABLE}.create_time ;;
  }

  dimension: delay_action {
    type: string
    sql: ${TABLE}.delay_action ;;
  }

  dimension: delay_action_description {
    type: string
    sql: ${TABLE}.delay_action_description ;;
  }

  dimension: delay_amount {
    type: number
    sql: ${TABLE}.delay_amount ;;
  }

  dimension: delay_direction {
    type: string
    sql: ${TABLE}.delay_direction ;;
  }

  dimension: delay_full_description {
    type: string
    sql: ${TABLE}.delay_full_description ;;
  }

  dimension: delay_type {
    type: string
    sql: ${TABLE}.delay_type ;;
  }

  dimension: email_id {
    type: string
    sql: ${TABLE}.email_id ;;
  }

  dimension: emails_sent {
    type: number
    sql: ${TABLE}.emails_sent ;;
  }

  dimension: event {
    type: string
    sql: ${TABLE}.event ;;
  }

  dimension: event_text {
    type: string
    sql: ${TABLE}.event_text ;;
  }

  dimension_group: ingested {
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
    sql: ${TABLE}.ingested_at ;;
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

  dimension: position {
    type: number
    sql: ${TABLE}.position ;;
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

  dimension: recipients_list_id {
    type: string
    sql: ${TABLE}.recipients_list_id ;;
  }

  dimension: recipients_list_is_active {
    type: yesno
    sql: ${TABLE}.recipients_list_is_active ;;
  }

  dimension: recipients_list_name {
    type: string
    sql: ${TABLE}.recipients_list_name ;;
  }

  dimension: report_summary_click_rate {
    type: number
    sql: ${TABLE}.report_summary_click_rate ;;
  }

  dimension: report_summary_clicks {
    type: number
    sql: ${TABLE}.report_summary_clicks ;;
  }

  dimension: report_summary_open_rate {
    type: number
    sql: ${TABLE}.report_summary_open_rate ;;
  }

  dimension: report_summary_opens {
    type: number
    sql: ${TABLE}.report_summary_opens ;;
  }

  dimension: report_summary_subscriber_clicks {
    type: number
    sql: ${TABLE}.report_summary_subscriber_clicks ;;
  }

  dimension: report_summary_unique_opens {
    type: number
    sql: ${TABLE}.report_summary_unique_opens ;;
  }

  dimension_group: send {
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
    sql: ${TABLE}.send_time ;;
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

  dimension: settings_from_name {
    type: string
    sql: ${TABLE}.settings_from_name ;;
  }

  dimension: settings_preview_text {
    type: string
    sql: ${TABLE}.settings_preview_text ;;
  }

  dimension: settings_reply_to {
    type: string
    sql: ${TABLE}.settings_reply_to ;;
  }

  dimension: settings_subject_line {
    type: string
    sql: ${TABLE}.settings_subject_line ;;
  }

  dimension: settings_template_id {
    type: number
    sql: ${TABLE}.settings_template_id ;;
  }

  dimension: settings_title {
    type: string
    sql: ${TABLE}.settings_title ;;
  }

  dimension: source {
    type: string
    sql: ${TABLE}.source ;;
  }

  dimension_group: start {
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
    sql: ${TABLE}.start_time ;;
  }

  dimension: status {
    type: string
    sql: ${TABLE}.status ;;
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

  dimension: tracking_google_analytics {
    type: string
    sql: ${TABLE}.tracking_google_analytics ;;
  }

  dimension: tracking_html_clicks {
    type: yesno
    sql: ${TABLE}.tracking_html_clicks ;;
  }

  dimension: tracking_opens {
    type: yesno
    sql: ${TABLE}.tracking_opens ;;
  }

  dimension: tracking_text_clicks {
    type: yesno
    sql: ${TABLE}.tracking_text_clicks ;;
  }

  dimension: trigger_settings_runtime_days {
    type: string
    sql: ${TABLE}.trigger_settings_runtime_days ;;
  }

  dimension: trigger_settings_runtime_hours_type {
    type: string
    sql: ${TABLE}.trigger_settings_runtime_hours_type ;;
  }

  dimension: trigger_settings_workflow_emails_count {
    type: number
    sql: ${TABLE}.trigger_settings_workflow_emails_count ;;
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

  dimension: web_id {
    type: number
    sql: ${TABLE}.web_id ;;
  }

  dimension: workflow_id {
    type: string
    sql: ${TABLE}.workflow_id ;;
  }

  measure: count {
    type: count
    drill_fields: [id, context_library_name, settings_from_name, recipients_list_name]
  }
}
