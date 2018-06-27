view: customers_analytics {
  sql_table_name: customers.analytics ;;

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
    type: number
    sql: ${TABLE}.total_free_trials ;;
  }

  measure: total_free_trials_sum {
    type: count_distinct
    sql: ${TABLE}.total_free_trials ;;
  }

  dimension: total_paying {
    type: number
    sql: ${TABLE}.total_paying ;;
  }

  measure: count {
    type: count
    drill_fields: []
  }
}
