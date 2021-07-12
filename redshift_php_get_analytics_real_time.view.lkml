view: redshift_php_get_analytics_real_time {
  derived_table: {
    sql: SELECT * FROM php.get_analytics_real_time ORDER BY timestamp DESC LIMIT 1
      ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: id {
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

  dimension_group: analytics_timestamp {
    type: time
    sql: ${TABLE}.analytics_timestamp ;;
  }

  dimension: context_library_consumer {
    type: string
    sql: ${TABLE}.context_library_consumer ;;
  }

  dimension: count_ {
    type: number
    sql: ${TABLE}.count ;;
  }

  dimension: existing_free_trials {
    type: string
    sql: ${TABLE}.existing_free_trials ;;
  }

  dimension: free_trial_created {
    type: string
    sql: ${TABLE}.free_trial_created ;;
  }

  dimension_group: ingest_date {
    type: time
    sql: ${TABLE}.ingest_date ;;
  }

  dimension: paused_created {
    type: string
    sql: ${TABLE}.paused_created ;;
  }

  dimension: paying_created {
    type: string
    sql: ${TABLE}.paying_created ;;
  }

  dimension_group: sent_at {
    type: time
    sql: ${TABLE}.sent_at ;;
  }

  dimension_group: timestamp {
    type: time
    sql: ${TABLE}.timestamp ;;
  }

  dimension: total_free_trials {
    type: string
    sql: ${TABLE}.total_free_trials ;;
  }

  dimension: context_library_name {
    type: string
    sql: ${TABLE}.context_library_name ;;
  }

  dimension: event {
    type: string
    sql: ${TABLE}.event ;;
  }

  dimension: paying_churn {
    type: string
    sql: ${TABLE}.paying_churn ;;
  }

  dimension: total_paying {
    type: string
    sql: ${TABLE}.total_paying ;;
  }

  dimension_group: uuid_ts {
    type: time
    sql: ${TABLE}.uuid_ts ;;
  }

  dimension: context_library_version {
    type: string
    sql: ${TABLE}.context_library_version ;;
  }

  dimension: event_text {
    type: string
    sql: ${TABLE}.event_text ;;
  }

  dimension: existing_paying {
    type: string
    sql: ${TABLE}.existing_paying ;;
  }

  dimension: free_trial_churn {
    type: string
    sql: ${TABLE}.free_trial_churn ;;
  }

  dimension: free_trial_converted {
    type: string
    sql: ${TABLE}.free_trial_converted ;;
  }

  dimension_group: original_timestamp {
    type: time
    sql: ${TABLE}.original_timestamp ;;
  }

  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
  }

  dimension: trend_metric_free_trial_growth {
    type: string
    sql: ${TABLE}.trend_metric_free_trial_growth ;;
  }

  dimension: trend_metric_subscriber_growth {
    type: string
    sql: ${TABLE}.trend_metric_subscriber_growth ;;
  }

  dimension: trend_metric_free_trial_conversion {
    type: string
    sql: ${TABLE}.trend_metric_free_trial_conversion ;;
  }

  dimension: trend_metric_churn {
    type: string
    sql: ${TABLE}.trend_metric_churn ;;
  }

  set: detail {
    fields: [
      id,
      received_at_time,
      uuid,
      analytics_timestamp_time,
      context_library_consumer,
      count_,
      existing_free_trials,
      free_trial_created,
      ingest_date_time,
      paused_created,
      paying_created,
      sent_at_time,
      timestamp_time,
      total_free_trials,
      context_library_name,
      event,
      paying_churn,
      total_paying,
      uuid_ts_time,
      context_library_version,
      event_text,
      existing_paying,
      free_trial_churn,
      free_trial_converted,
      original_timestamp_time,
      user_id,
      trend_metric_free_trial_growth,
      trend_metric_subscriber_growth,
      trend_metric_free_trial_conversion,
      trend_metric_churn
    ]
  }
}
