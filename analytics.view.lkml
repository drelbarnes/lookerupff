view: analytics {
  sql_table_name: customers.analytics ;;

  dimension: existing_free_trials {
    type: number
    sql: ${TABLE}.existing_free_trials ;;
  }

  measure: total_active_free_trial_subs {
    type: sum
    description: "Total number of active subs during a time period."
    sql:  ${existing_free_trials} ;;
    drill_fields: [timestamp_date, existing_free_trials]
  }

  dimension: existing_paying {
    type: number
    sql: ${TABLE}.existing_paying ;;
  }

  measure: total_active_paid_subs {
    type: sum
    description: "Total active number of paid subs during a time period."
    sql:  ${existing_paying} ;;
    drill_fields: [timestamp_date, existing_paying]
  }

  dimension: free_trial_churn {
    type: number
    sql: ${TABLE}.free_trial_churn ;;
  }

  measure: average_churn_by {
    type: average
    description: "Average churn in a given time period."
    sql:  ${free_trial_churn} / ${free_trial_created} ;;
    drill_fields: [timestamp_date, average_churn_by]
  }

  measure: new_cancelled_trials {
    type: sum
    description: "Total number of cancelled trials during a time period."
    sql:  ${free_trial_churn} ;;
    drill_fields: [timestamp_date, free_trial_churn]
  }


  dimension: free_trial_converted {
    type: number
    sql: ${TABLE}.free_trial_converted ;;
  }

  measure: trial_to_paid {
    type: sum
    description: "Total number of trials to paid during a time period."
    sql:  ${free_trial_converted} ;;
  }


  dimension: free_trial_created {
    type: number
    sql: ${TABLE}.free_trial_created ;;
  }

  measure: new_trials {
    type: sum
    description: "Total number of new trials during a time period."
    sql:  ${free_trial_created} ;;
  }

  measure: total_active_subs {
    type: sum
    description: "Total active subs"
    sql:  ${existing_paying} + ${existing_free_trials} ;;
  }

  dimension: paused_created {
    type: number
    sql: ${TABLE}.paused_created ;;
  }

  dimension: paying_created {
    type: number
    sql: ${TABLE}.paying_created ;;
  }

  dimension: paying_churn {
    type: number
    sql: ${TABLE}.paying_churn ;;
  }

  measure: new_cancelled_paid {
    type: sum
    description: "Total number of cancelled paid subs during a time period."
    sql:  ${paying_churn} ;;
    drill_fields: [timestamp_date, paying_churn]
  }

  measure: new_paid {
    type: sum
    description: "Total number of new paids during a time period."
    sql:  ${paying_created} ;;
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

  dimension: total_paying {
    type: number
    sql: ${TABLE}.total_paying ;;
  }

  measure: count {
    type: count
    drill_fields: []
  }

# ------
# Filters
# ------

## filter determining time range for all "A" measures
  filter: time_a {
    type: date_time
  }

## flag for "A" measures to only include appropriate time range
  dimension: group_a {
    hidden: yes
    type: yesno
    sql: {% condition time_a %} ${timestamp_raw} {% endcondition %}
      ;;
  }

  measure: count_a {
    type: sum
    sql:  ${free_trial_created} ;;
    filters: {
      field: group_a
      value: "yes"
    }
  }

## filter determining time range for all "B" measures
  filter: time_b {
    type: date_time
  }

## flag for "B" measures to only include appropriate time range
  dimension: group_b {
    hidden: yes
    type: yesno
    sql: {% condition time_b %} ${timestamp_raw} {% endcondition %}
      ;;
  }

  measure: count_b {
    type: sum
    sql:  ${free_trial_created} ;;
   filters: {
    field: group_b
    value: "yes"
  }
}

## filter on comparison queries to avoid querying unnecessarily large date ranges.
  dimension: is_in_time_a_or_b {
    group_label: "Time Comparison Filters"
    type: yesno
    sql: {% condition time_a %} ${timestamp_raw} {% endcondition %}
          OR {% condition time_b %} ${timestamp_raw} {% endcondition %}
           ;;
  }


}
