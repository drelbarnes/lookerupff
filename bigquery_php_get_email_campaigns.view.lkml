view: bigquery_php_get_email_campaigns {
  derived_table: {
    sql: SELECT * FROM `up-faith-and-family-216419.php.get_email_campaigns`
      ;;
  }

  dimension: id {
    primary_key: yes
    type: string
    tags: ["user_id"]
    sql: ${TABLE}.id ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: abuse_reports {
    type: number
    sql: ${TABLE}.abuse_reports ;;
  }

  dimension: bounces_hard {
    type: number
    sql: ${TABLE}.bounces_hard ;;
  }

  dimension: bounces_soft {
    type: number
    sql: ${TABLE}.bounces_soft ;;
  }

  dimension: bounces_syntax_errors {
    type: number
    sql: ${TABLE}.bounces_syntax_errors ;;
  }

  dimension: campaign_email_sent {
    type: number
    sql: ${TABLE}.campaign_email_sent ;;
  }

  dimension: campaign_id {
    type: string
    sql: ${TABLE}.campaign_id ;;
  }

  dimension: campaign_preview_text {
    type: string
    sql: ${TABLE}.campaign_preview_text ;;
  }

  dimension_group: campaign_send_time {
    type: time
    sql: ${TABLE}.campaign_send_time ;;
  }

  dimension: campaign_subject_line {
    type: string
    sql: ${TABLE}.campaign_subject_line ;;
  }

  dimension: campaign_title {
    type: string
    sql: ${TABLE}.campaign_title ;;
  }

  dimension: campaign_type {
    type: string
    sql: ${TABLE}.campaign_type ;;
  }

  dimension: campaign_unsubscribed {
    type: number
    sql: ${TABLE}.campaign_unsubscribed ;;
  }

  dimension_group: clicks_last_click {
    type: time
    sql: ${TABLE}.clicks_last_click ;;
  }

  dimension: clicks_total {
    type: number
    sql: ${TABLE}.clicks_total ;;
  }

  dimension: clicks_unique {
    type: number
    sql: ${TABLE}.clicks_unique ;;
  }

  dimension: clicks_unique_subscriber_clicks {
    type: number
    sql: ${TABLE}.clicks_unique_subscriber_clicks ;;
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

  dimension: email_sent {
    type: number
    sql: ${TABLE}.email_sent ;;
  }

  dimension: event {
    type: string
    sql: ${TABLE}.event ;;
  }

  dimension: event_text {
    type: string
    sql: ${TABLE}.event_text ;;
  }

  dimension: industry_stats_abuse_rate {
    type: number
    sql: ${TABLE}.industry_stats_abuse_rate ;;
  }

  dimension: industry_stats_bounce_rate {
    type: number
    sql: ${TABLE}.industry_stats_bounce_rate ;;
  }

  dimension: industry_stats_click_rate {
    type: number
    sql: ${TABLE}.industry_stats_click_rate ;;
  }

  dimension: industry_stats_open_rate {
    type: number
    sql: ${TABLE}.industry_stats_open_rate ;;
  }

  dimension: industry_stats_type {
    type: string
    sql: ${TABLE}.industry_stats_type ;;
  }

  dimension: industry_stats_unopen_rate {
    type: number
    sql: ${TABLE}.industry_stats_unopen_rate ;;
  }

  dimension: industry_stats_unsub_rate {
    type: number
    sql: ${TABLE}.industry_stats_unsub_rate ;;
  }

  dimension_group: ingested_at {
    type: time
    sql: ${TABLE}.ingested_at ;;
  }

  dimension: list_id {
    type: string
    sql: ${TABLE}.list_id ;;
  }

  dimension: list_id_active {
    type: string
    sql: ${TABLE}.list_id_active ;;
  }

  dimension: list_name {
    type: string
    sql: ${TABLE}.list_name ;;
  }

  dimension: list_stats_click_rate {
    type: number
    sql: ${TABLE}.list_stats_click_rate ;;
  }

  dimension: list_stats_open_rate {
    type: number
    sql: ${TABLE}.list_stats_open_rate ;;
  }

  dimension: list_stats_sub_rate {
    type: number
    sql: ${TABLE}.list_stats_sub_rate ;;
  }

  dimension: list_stats_unsub_rate {
    type: number
    sql: ${TABLE}.list_stats_unsub_rate ;;
  }

  dimension_group: loaded_at {
    type: time
    sql: ${TABLE}.loaded_at ;;
  }

  dimension_group: opens_last_open {
    type: time
    sql: ${TABLE}.opens_last_open ;;
  }

  dimension: opens_open_rate {
    type: number
    sql: ${TABLE}.opens_open_rate ;;
  }

  dimension: opens_total {
    type: number
    sql: ${TABLE}.opens_total ;;
  }

  dimension: opens_unique {
    type: number
    sql: ${TABLE}.opens_unique ;;
  }

  dimension_group: original_timestamp {
    type: time
    sql: ${TABLE}.original_timestamp ;;
  }

  dimension: preview_text {
    type: string
    sql: ${TABLE}.preview_text ;;
  }

  dimension_group: received_at {
    type: time
    sql: ${TABLE}.received_at ;;
  }

  dimension_group: send_time {
    type: time
    sql: ${TABLE}.send_time ;;
  }

  dimension_group: sent_at {
    type: time
    sql: ${TABLE}.sent_at ;;
  }

  dimension: subject_line {
    type: string
    sql: ${TABLE}.subject_line ;;
  }

  dimension_group: timestamp {
    type: time
    sql: ${TABLE}.timestamp ;;
  }

  dimension: unsubscribed {
    type: number
    sql: ${TABLE}.unsubscribed ;;
  }

  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
  }

  dimension_group: uuid_ts {
    type: time
    sql: ${TABLE}.uuid_ts ;;
  }

  dimension: source {
    type: string
    sql: ${TABLE}.source ;;
  }

  measure: unique_email_opens {
    type: sum
    sql: ${opens_unique} ;;
  }

  measure: campaign_open_rate {
    type: average
    value_format: "0\%"
    sql: ${opens_open_rate} * 100 ;;
  }

  measure: open_rate {
    type: average
    value_format: "0\%"
    sql: (${opens_open_rate} * 100);;
    filters: [list_id: "fc061462da"]
  }

  measure: list_open_rate {
    type: number
    value_format: "0\%"
    sql: ${list_stats_open_rate} ;;
  }

  measure: industry_open_rate {
    type: number
    value_format: "0\%"
    sql: ${industry_stats_open_rate} * 100 ;;
  }

  measure: unique_email_clicks {
    type: sum
    sql: ${clicks_unique} ;;
  }

  measure:  bounces_total {
    type: sum
    sql: ${bounces_hard} + ${bounces_soft} ;;
  }

  measure: unsubscribed_total {
    type: sum
    sql:  ${campaign_unsubscribed};;
  }

  measure:  email_send_count {
    type: number
    sql: ${campaign_email_sent} ;;
  }

  set: detail {
    fields: [
      abuse_reports,
      bounces_hard,
      bounces_soft,
      bounces_syntax_errors,
      campaign_email_sent,
      campaign_id,
      campaign_preview_text,
      campaign_send_time_time,
      campaign_subject_line,
      campaign_title,
      campaign_type,
      campaign_unsubscribed,
      clicks_last_click_time,
      clicks_total,
      clicks_unique,
      clicks_unique_subscriber_clicks,
      context_library_consumer,
      context_library_name,
      context_library_version,
      email_sent,
      event,
      event_text,
      id,
      industry_stats_abuse_rate,
      industry_stats_bounce_rate,
      industry_stats_click_rate,
      industry_stats_open_rate,
      industry_stats_type,
      industry_stats_unopen_rate,
      industry_stats_unsub_rate,
      ingested_at_time,
      list_id,
      list_id_active,
      list_name,
      list_stats_click_rate,
      list_stats_open_rate,
      list_stats_sub_rate,
      list_stats_unsub_rate,
      loaded_at_time,
      opens_last_open_time,
      opens_open_rate,
      opens_total,
      opens_unique,
      original_timestamp_time,
      preview_text,
      received_at_time,
      send_time_time,
      sent_at_time,
      subject_line,
      timestamp_time,
      unsubscribed,
      user_id,
      uuid_ts_time,
      source
    ]
  }
}
