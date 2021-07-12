view: redshift_php_get_analytics_real_time {
  sql_table_name: php.get_analytics_real_time ;;
  drill_fields: [id]

  dimension: id {
    primary_key: yes
    type: string
    sql: ${TABLE}.id ;;
  }

  dimension_group: analytics_timestamp {
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
    sql: ${TABLE}.analytics_timestamp ;;
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

  dimension: count_get_analytics_real_time {
    type: number
    sql: ${TABLE}.count ;;
  }

  dimension: event {
    type: string
    sql: ${TABLE}.event ;;
  }

  dimension: event_text {
    type: string
    sql: ${TABLE}.event_text ;;
  }

  dimension: existing_free_trials {
    type: string
    sql: ${TABLE}.existing_free_trials ;;
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

  dimension: free_trial_created {
    type: string
    sql: ${TABLE}.free_trial_created ;;
  }

  dimension_group: ingest {
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
    sql: ${TABLE}.ingest_date ;;
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

  dimension: paused_created {
    type: string
    sql: ${TABLE}.paused_created ;;
  }

  dimension: paying_churn {
    type: string
    sql: ${TABLE}.paying_churn ;;
  }

  dimension: paying_created {
    type: string
    sql: ${TABLE}.paying_created ;;
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

  dimension: total_free_trials {
    type: string
    sql: ${TABLE}.total_free_trials ;;
  }

  dimension: total_paying {
    type: string
    sql: ${TABLE}.total_paying ;;
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
    drill_fields: [id, context_library_name]
  }
}
