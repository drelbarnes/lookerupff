view: vimeo_ott_customer_record_analytics {
  # Or, you could make this view a derived table, like this:
  derived_table: {
    sql: with analytics AS (
        SELECT
          date(sc.report_date)-1 as "timestamp",
          sc.platform,
          sc.frequency,
          COUNT(DISTINCT CASE WHEN sc.state_change = 'free_trial_created' THEN sc.user_id END) AS free_trial_created,
          COUNT(DISTINCT CASE WHEN sc.state_change = 'free_trial_converted' THEN sc.user_id END) AS free_trial_converted,
          COUNT(DISTINCT CASE WHEN sc.state_change = 'free_trial_churn' THEN sc.user_id END) AS free_trial_churn,
          COUNT(DISTINCT CASE WHEN sc.state_change = 'paying_created' THEN sc.user_id END) AS paying_created,
          COUNT(DISTINCT CASE WHEN sc.state_change = 'paying_churn' THEN sc.user_id END) AS paying_churn,
          COUNT(DISTINCT CASE WHEN sc.state_change = 'paused_created' THEN sc.user_id END) AS paused_created,
          -- Correct the calculation for total_paying and total_free_trials
          COUNT(DISTINCT CASE WHEN sc.status = 'enabled' THEN sc.user_id END) AS total_paying,
          COUNT(DISTINCT CASE WHEN sc.status = 'free_trial' THEN sc.user_id END) AS total_free_trials
        FROM
          ${vimeo_ott_customer_record.SQL_TABLE_NAME} sc
        GROUP BY
          sc.report_date,
          sc.platform,
          sc.frequency
      )
      , expanded_analytics as (
        select *
        , LAG(free_trial_created, 14) over (partition by platform, frequency order by date("timestamp")) as new_trials_14_days_prior
        , sum(paying_churn) over (partition by platform, frequency order by date("timestamp") ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) as churn_30_days
        , LAG(total_paying, 30) over (partition by platform, frequency order by date("timestamp")) as paying_30_days_prior
        , sum(paying_churn) over (partition by platform, frequency order by date("timestamp") ROWS BETWEEN 364 PRECEDING AND CURRENT ROW) as churn_365_days
        , LAG(total_paying, 365) over (partition by platform, frequency order by date("timestamp")) as paying_365_days_prior
      from analytics
      )
      select * from expanded_analytics
      ;;
  }

  dimension_group: timestamp {
    type: time
    sql: ${TABLE}."timestamp" ;;
  }

  dimension: datestamp {
    type: date
    primary_key: yes
    sql: date(${TABLE}."timestamp") ;;
  }

  dimension: platform {
    type: string
    sql: ${TABLE}.platform ;;
  }

  dimension: frequency {
    type: string
    sql: ${TABLE}.frequency ;;
  }

  dimension: total_free_trials {
    type: number
    sql: ${TABLE}.total_free_trials ;;
  }

  dimension: total_paying {
    type: number
    sql: ${TABLE}.total_paying ;;
  }

  dimension: free_trial_created {
    type: number
    sql: ${TABLE}.free_trial_created ;;
  }

  dimension: free_trial_converted {
    type: number
    sql: ${TABLE}.free_trial_converted ;;
  }

  dimension: free_trial_churn {
    type: number
    sql: ${TABLE}.free_trial_churn ;;
  }

  dimension: paying_created {
    type: number
    sql: ${TABLE}.paying_created ;;
  }

  dimension: paying_churn {
    type: number
    sql: ${TABLE}.paying_churn ;;
  }

  dimension: paused_created {
    type: number
    sql: ${TABLE}.paused_created ;;
  }

  dimension: new_trials_14_days_prior {
    type: number
    sql: ${TABLE}.new_trials_14_days_prior ;;
  }

  dimension: churn_30_days {
    type: number
    sql: ${TABLE}.churn_30_days ;;
  }

  dimension: churn_365_days {
    type: number
    sql: ${TABLE}.churn_365_days ;;
  }

  dimension: paying_30_days_prior {
    type: number
    sql: ${TABLE}.paying_30_days_prior ;;
  }

  dimension: paying_365_days_prior {
    type: number
    sql: ${TABLE}.paying_365_days_prior ;;
  }

  measure: new_trials {
    type: sum
    label: "New Trials"
    description: "Total number of new trials during period."
    sql:  ${free_trial_created} ;;
  }

  measure: total_new_trials_14_days_prior {
    type: sum
    label: "New Trials (14 days prior)"
    sql: ${new_trials_14_days_prior};;
  }

  measure: trial_to_paid {
    type: sum
    label: "Trial to Paid"
    description: "Total number of trials converted to paid subscribers during period."
    sql:  ${free_trial_converted} ;;
  }

  measure: conversion_rate {
    type: number
    label: "Free Trial Conversion Rate"
    value_format_name: percent_2
    sql: ${trial_to_paid}/NULLIF(${total_new_trials_14_days_prior},0) ;;
  }

  measure: new_paid {
    type: sum
    description: "Total number of new paids during period."
    sql:  ${paying_created} ;;
  }

  measure: paid_churn {
    type: sum
    sql: ${paying_churn} ;;
  }

  measure: churn_30_days_ {
    type: sum
    label: "Churn 30 Days"
    sql: ${churn_30_days} ;;
  }

  measure: paying_30_days_prior_ {
    type: sum
    label: "Paying_30_days_prior"
    sql: ${paying_30_days_prior} ;;
  }

  measure: churn_30_day_percent {
    type: sum
    label: "Churn Rate"
    sql: ${churn_30_days}/${paying_30_days_prior};;
    value_format_name: percent_1
  }

  measure: churn_365_days_ {
    type: sum
    label: "Churn 365 Days"
    sql: ${churn_365_days} ;;
  }

  measure: paying_365_days_prior_ {
    type: sum
    label: "Paying_365_days_prior"
    sql: ${paying_365_days_prior} ;;
  }

  measure: churn_365_day_percent {
    type: sum
    label: "Churn Rate"
    sql: ${churn_365_days}/${paying_365_days_prior};;
    value_format_name: percent_1
  }

}
