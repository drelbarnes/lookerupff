view: analytics_v2 {
  derived_table: {
    sql: select a.*,
                case when rownum=max(rownum) over(partition by Week) then existing_paying end as PriorWeekExistingSubs,
                case when rownum=max(rownum) over(partition by Month) then existing_paying end as PriorMonthExistingSubs,
                wait_content,
                save_money,
                vacation,
                high_price,
                other
                from
      ((select a.*,cast(datepart(week,date(timestamp)) as varchar) as Week,
      cast(datepart(month,date(timestamp)) as varchar) as Month,
      cast(datepart(Quarter,date(timestamp)) as varchar) as Quarter,
      cast(datepart(Year,date(timestamp)) as varchar) as Year,
      new_trials_14_days_prior from
      (select *, row_number() over(order by timestamp desc) as rownum from customers.analytics) as a
      left join
      (select free_trial_created as new_trials_14_days_prior, row_number() over(order by timestamp desc) as rownum from customers.analytics
      where timestamp in
                      (select dateadd(day,-15,timestamp) as timestamp from customers.analytics )) as b on a.rownum=b.rownum)) as a
      left join customers.churn_reasons_aggregated as b on a.timestamp=b.timestamp;;}

  dimension: high_price {
    type: number
    sql: ${TABLE}.high_price ;;
  }

  dimension: other {
    type: number
    sql: ${TABLE}.other ;;
  }

  dimension: save_money {
    type: string
    sql: ${TABLE}.save_money ;;
  }

  measure: high_price_total {
    type: sum
    sql: ${TABLE}.high_price ;;
  }

  measure: other_total {
    type: sum
    sql: ${TABLE}.other ;;
  }

  measure: save_money_total {
    type: sum
    sql: ${TABLE}.save_money ;;
  }

  dimension: vacation {
    type: number
    sql: ${TABLE}.vacation ;;
  }

  dimension: wait_content {
    type: number
    sql: ${TABLE}.wait_content ;;
  }

  measure: vacation_total {
    type: sum
    sql: ${TABLE}.vacation ;;
  }

  measure: wait_content_total {
    type: sum
    sql: ${TABLE}.wait_content ;;
  }

  dimension: new_trials_14_days_prior{
    type: number
    sql: ${TABLE}.new_trials_14_days_prior;;
  }

  dimension: conversion {
    type: number
    value_format: ".0#\%"
    sql: 100.0*${TABLE}.free_trial_converted/${TABLE}.new_trials_14_days_prior ;;
  }

  measure: total_new_trials_14_days_prior {
    type: sum
    sql: ${TABLE}.new_trials_14_days_prior;;
  }

  dimension: existing_free_trials {
    type: number
    sql: ${TABLE}.existing_free_trials ;;
  }

  measure: total_active_free_trials {
    type: sum
    sql:${existing_free_trials} ;;
  }

  dimension: existing_paying {
    type: number
    sql: ${TABLE}.existing_paying ;;
  }

  measure: total_active_paying {
    type: sum
    sql: ${existing_paying} ;;
  }

  measure: total_active_subs {
    type: number
    sql: ${existing_free_trials} + ${existing_paying} ;;
  }


  dimension: free_trial_churn {
    type: number
    sql: ${TABLE}.free_trial_churn ;;
  }

  measure: new_cancelled_trials {
    type: sum
    description: "Total number of cancelled trials during a time period."
    sql:  ${free_trial_churn} ;;
    drill_fields: [timestamp_date, free_trial_churn]
  }

  measure: cancelled_trials {
    type: sum
    description: "Total number of cancelled trials during a time period."
    sql:  ${free_trial_churn}*-1 ;;
    drill_fields: [timestamp_date, free_trial_churn]
  }

  measure: free_trials_count {
    type: sum
    description: "Total number of existing trials during a period of time"
    sql:  ${existing_free_trials} ;;
  }

  measure: paid_subs_count {
    type: sum
    description: "Total number of existing paid subs during a period of time"
    sql:  ${existing_paying} ;;
  }

  measure: total_count {
    type: sum
    description: "Total number of existing free trials and paid subs during a period of time"
    sql:  ${existing_paying}+${existing_free_trials} ;;
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

  measure: total_cancelled {
    type: sum
    description: "Total number of cancelled free trials and paid subs during a time period."
    sql: ${paying_churn}+${free_trial_churn} ;;
  }
  measure: new_paid {
    type: sum
    description: "Total number of new paids during a time period."
    sql:  ${paying_created} ;;
  }

  measure: new_total {
    type: sum
    description: "Total number of new free trials and paid subs during a time period."
    sql:  ${paying_created}+${free_trial_created}+${free_trial_converted};;
  }

  measure:  new_paid_total{
    type: sum
    description: "Total number of new paid subs (reacquisitions) and free trial to paid."
    sql: ${free_trial_converted}+${paying_created};;
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

  measure: paying_total {
    type: sum
    sql: ${TABLE}.total_paying ;;
  }

  measure: free_trials_total {
    type: sum
    sql: ${TABLE}.total_free_trials ;;
  }

  dimension: churn_rate {
    type: number
    value_format: ".0#\%"
    sql: 100.0*${TABLE}.paying_churn/${TABLE}.existing_paying ;;
  }

  dimension: rownum {
    type: number
    sql: {TABLE}.rownum ;;
  }

  measure: minrow {
    type: min
    sql: ${TABLE}.rownum ;;
  }

  measure: last_updated_date {
    type: date
    sql: MAX(${timestamp_raw});;
  }

measure: end_of_prior_week_subs {
  type: sum
  sql: ${TABLE}.PriorWeekExistingSubs ;;
}

  measure: end_of_prior_month_subs {
    type: sum
    sql: ${TABLE}.PriorMonthExistingSubs ;;
  }

  measure: weekly_churn {
    type: number
    value_format: ".0#\%"
    sql: 100.0*${new_cancelled_paid}/${end_of_prior_week_subs} ;;
  }

  measure: monthly_churn {
    type: number
    sql: ${new_cancelled_paid}/${end_of_prior_month_subs} ;;
  }

  measure: count {
    type: count
    drill_fields: []
  }

  measure: PaidTrialLost {
    type: sum
    sql: ${paying_created}-${paying_churn}  ;;

  }

  measure: Cancelled_Subs {
    type: sum
    sql: ${paying_churn}*-1 ;;
  }

  measure: conversion_rate_v2 {
    type: number
    value_format: ".0#\%"
    sql: 100.0*${trial_to_paid}/${total_new_trials_14_days_prior} ;;
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

  measure: free_trial_created_14_days_prior {
    type: sum
    sql:  ${free_trial_created} ;;
    filters: {
      field: group_a
      value: "yes"
    }
  }

  measure: new_trial_14_days_prior {
    type: sum
    sql:  ${free_trial_created}-14 ;;
  }

  measure: free_trial_converted_today {
    type: sum
    sql:  ${free_trial_converted} ;;
    filters: {
      field: group_b
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

  dimension: is_in_time_a {
    group_label: "Group A Comparison Filter"
    type: yesno
    sql:{% condition time_a %} ${timestamp_raw} {% endcondition %};;
  }

  dimension: is_in_time_b {
    group_label: "Group B Comparison Filter"
    type: yesno
    sql:{% condition time_b %} ${timestamp_raw} {% endcondition %};;
  }
}
