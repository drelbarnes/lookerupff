view: bigquery_analytics {
  derived_table: {
    sql: (select analytics_timestamp as timestamp,
       existing_free_trials,
       existing_paying,
       free_trial_churn,
       free_trial_converted,
       free_trial_created,
       paused_created,
       paying_churn,
       paying_created,
       total_free_trials,
       total_paying
from php.get_analytics
where date(sent_at)=current_date())
 ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension_group: timestamp {
    type: time
    sql: ${TABLE}.timestamp ;;
  }

  dimension: existing_free_trials {
    type: number
    sql: ${TABLE}.existing_free_trials ;;
  }

  dimension: existing_paying {
    type: number
    sql: ${TABLE}.existing_paying ;;
  }

  dimension: free_trial_churn {
    type: number
    sql: ${TABLE}.free_trial_churn ;;
  }

  dimension: free_trial_converted {
    type: number
    sql: ${TABLE}.free_trial_converted ;;
  }

  dimension: free_trial_created {
    type: number
    sql: ${TABLE}.free_trial_created ;;
  }

  dimension: paused_created {
    type: number
    sql: ${TABLE}.paused_created ;;
  }

  dimension: paying_churn {
    type: number
    sql: ${TABLE}.paying_churn ;;
  }

  dimension: paying_created {
    type: number
    sql: ${TABLE}.paying_created ;;
  }

  dimension: total_free_trials {
    type: number
    sql: ${TABLE}.total_free_trials ;;
  }

  dimension: total_paying {
    type: number
    sql: ${TABLE}.total_paying ;;
  }

  measure: existing_free_trials_ {
    type: sum
    sql: ${TABLE}.existing_free_trials ;;
  }

  measure: existing_paying_ {
    type: sum
    sql: ${TABLE}.existing_paying ;;
  }

  measure: free_trial_churn_ {
    type: sum
    sql: ${TABLE}.free_trial_churn ;;
  }

  measure: free_trial_converted_ {
    type: sum
    sql: ${TABLE}.free_trial_converted ;;
  }

  measure: free_trial_created_ {
    type: sum
    sql: ${TABLE}.free_trial_created ;;
  }

  measure: paused_created_ {
    type: sum
    sql: ${TABLE}.paused_created ;;
  }

  measure: paying_churn_ {
    type: sum
    sql: ${TABLE}.paying_churn ;;
  }

  measure: paying_created_ {
    type: sum
    sql: ${TABLE}.paying_created ;;
  }

 measure: total_free_trials_ {
    type: sum
    sql: ${TABLE}.total_free_trials ;;
  }

  measure: total_paying_ {
    type: sum
    sql: ${TABLE}.total_paying ;;
  }

  set: detail {
    fields: [
      timestamp_time,
      existing_free_trials,
      existing_paying,
      free_trial_churn,
      free_trial_converted,
      free_trial_created,
      paused_created,
      paying_churn,
      paying_created,
      total_free_trials,
      total_paying
    ]
  }
}
