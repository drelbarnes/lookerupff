view: analytics_v3 {
  # Or, you could make this view a derived table, like this:
  derived_table: {
    sql: WITH date_parameters AS (
        SELECT
        {% date_start user_defined_date_range %}::DATE AS date_start,
        {% date_end user_defined_date_range %}::DATE AS date_end,
        {% if user_defined_compare_to_range._is_filtered %}
        {% date_start user_defined_compare_to_range %}::DATE AS comparison_date_start,
        {% date_end user_defined_compare_to_range %}::DATE AS comparison_date_end
        {% else %}
        NULL::DATE AS comparison_date_start,
        NULL::DATE AS comparison_date_end
        {% endif %}
      ),
      date_delta AS (
        SELECT
        date_start,
        date_end,
        comparison_date_start,
        comparison_date_end,
        {% if user_defined_compare_to_range._is_filtered %}
        (date_start - comparison_date_start) AS delta_days
        {% else %}
        NULL::INTEGER AS delta_days
        {% endif %}
        FROM date_parameters
      )
      , analytics_v2 as (
        select
        "timestamp"
        , total_free_trials
        , total_paying
        , free_trial_created
        , free_trial_converted
        , free_trial_churn
        , paying_created
        , paying_churn
        , paused_created
        , new_trials_14_days_prior
        , churn_30_days
        , paying_30_days_prior
        , day_of_year
        , week
        , month
        , year
        , quarter
        , rownum
        from ${analytics_v2.SQL_TABLE_NAME}
      )
      , daily_spend as (
      select
      date_start
      , spend
      from ${daily_spend.SQL_TABLE_NAME}
      group by 1,2
      )
      , ltv_cpa as (
        select
        "timestamp"
        , cpa
        , ltv
        from ${ltv_cpa.SQL_TABLE_NAME}
        group by 1,2,3
      )
      , kpis as (
        select
        a."timestamp"
        , a.total_free_trials
        , a.total_paying
        , a.free_trial_created
        , a.free_trial_converted
        , a.free_trial_churn
        , a.paying_created
        , a.paying_churn
        , a.paused_created
        , a.new_trials_14_days_prior
        , a.churn_30_days
        , a.paying_30_days_prior
        , a.day_of_year
        , a.week
        , a.month
        , a.year
        , a.quarter
        , b.spend
        , c.cpa
        , c.ltv
        from analytics_v2 a
        left join daily_spend b
        on a."timestamp" = date(b.date_start)
        left join ltv_cpa c
        on a."timestamp" = c."timestamp"
        order by a."timestamp" desc
      )
      , comparison_kpis as (
        SELECT
        k."timestamp"
        {% if user_defined_compare_to_range._is_filtered %}
        , LAG(k."timestamp", dd.delta_days) OVER (ORDER BY k."timestamp") AS comparison_timestamp
        {% else %}
        , NULL::TIMESTAMP as comparison_timestamp
        {% endif %}
        , k.total_free_trials
        {% if user_defined_compare_to_range._is_filtered %}
        , LAG(k.total_free_trials, dd.delta_days) OVER (ORDER BY k."timestamp") AS comparison_total_free_trials
        {% else %}
        , NULL::INTEGER as comparison_total_free_trials
        {% endif %}
        , k.total_paying
        {% if user_defined_compare_to_range._is_filtered %}
        , LAG(k.total_paying, dd.delta_days) OVER (ORDER BY k."timestamp") AS comparison_total_paying
        {% else %}
        , NULL::INTEGER as comparison_total_paying
        {% endif %}
        , k.free_trial_created
        {% if user_defined_compare_to_range._is_filtered %}
        , LAG(k.free_trial_created, dd.delta_days) OVER (ORDER BY k."timestamp") AS comparison_free_trial_created
        {% else %}
        , NULL::INTEGER as comparison_free_trial_created
        {% endif %}
        , k.free_trial_converted
        {% if user_defined_compare_to_range._is_filtered %}
        , LAG(k.free_trial_converted, dd.delta_days) OVER (ORDER BY k."timestamp") AS comparison_free_trial_converted
        {% else %}
        , NULL::INTEGER as comparison_free_trial_converted
        {% endif %}
        , k.free_trial_churn
        {% if user_defined_compare_to_range._is_filtered %}
        , LAG(k.free_trial_churn, dd.delta_days) OVER (ORDER BY k."timestamp") AS comparison_free_trial_churn
        {% else %}
        , NULL::INTEGER as comparison_free_trial_churn
        {% endif %}
        , k.paying_created
        {% if user_defined_compare_to_range._is_filtered %}
        , LAG(k.paying_created, dd.delta_days) OVER (ORDER BY k."timestamp") AS comparison_paying_created
        {% else %}
        , NULL::INTEGER as comparison_paying_created
        {% endif %}
        , k.paying_churn
        {% if user_defined_compare_to_range._is_filtered %}
        , LAG(k.paying_churn, dd.delta_days) OVER (ORDER BY k."timestamp") AS comparison_paying_churn
        {% else %}
        , NULL::INTEGER as comparison_paying_churn
        {% endif %}
        , k.paused_created
        {% if user_defined_compare_to_range._is_filtered %}
        , LAG(k.paused_created, dd.delta_days) OVER (ORDER BY k."timestamp") AS comparison_paused_created
        {% else %}
        , NULL::INTEGER as comparison_paused_created
        {% endif %}
        , k.new_trials_14_days_prior
        {% if user_defined_compare_to_range._is_filtered %}
        , LAG(k.new_trials_14_days_prior, dd.delta_days) OVER (ORDER BY k."timestamp") AS comparison_new_trials_14_days_prior
        {% else %}
        , NULL::INTEGER as comparison_new_trials_14_days_prior
        {% endif %}
        , k.churn_30_days
        {% if user_defined_compare_to_range._is_filtered %}
        , LAG(k.churn_30_days, dd.delta_days) OVER (ORDER BY k."timestamp") AS comparison_churn_30_days
        {% else %}
        , NULL::INTEGER as comparison_churn_30_days
        {% endif %}
        , k.paying_30_days_prior
        {% if user_defined_compare_to_range._is_filtered %}
        , LAG(k.paying_30_days_prior, dd.delta_days) OVER (ORDER BY k."timestamp") AS comparison_paying_30_days_prior
        {% else %}
        , NULL::INTEGER as comparison_paying_30_days_prior
        {% endif %}
        , k.day_of_year
        {% if user_defined_compare_to_range._is_filtered %}
        , LAG(k.day_of_year, dd.delta_days) OVER (ORDER BY k."timestamp") AS comparison_day_of_year
        {% else %}
        , NULL::INTEGER as comparison_day_of_year
        {% endif %}
        , k.week
        {% if user_defined_compare_to_range._is_filtered %}
        , LAG(k.week, dd.delta_days) OVER (ORDER BY k."timestamp") AS comparison_week
        {% else %}
        , NULL::INTEGER as comparison_week
        {% endif %}
        , k.month
        {% if user_defined_compare_to_range._is_filtered %}
        , LAG(k.month, dd.delta_days) OVER (ORDER BY k."timestamp") AS comparison_month
        {% else %}
        , NULL::INTEGER as comparison_month
        {% endif %}
        , k.year
        {% if user_defined_compare_to_range._is_filtered %}
        , LAG(k.year, dd.delta_days) OVER (ORDER BY k."timestamp") AS comparison_year
        {% else %}
        , NULL::INTEGER as comparison_year
        {% endif %}
        , k.quarter
        {% if user_defined_compare_to_range._is_filtered %}
        , LAG(k.quarter, dd.delta_days) OVER (ORDER BY k."timestamp") AS comparison_quarter
        {% else %}
        , NULL::INTEGER as comparison_quarter
        {% endif %}
        , k.spend
        {% if user_defined_compare_to_range._is_filtered %}
        , LAG(k.spend, dd.delta_days) OVER (ORDER BY k."timestamp") AS comparison_spend
        {% else %}
        , NULL::INTEGER as comparison_spend
        {% endif %}
        , k.cpa
        {% if user_defined_compare_to_range._is_filtered %}
        , LAG(k.cpa, dd.delta_days) OVER (ORDER BY k."timestamp") AS comparison_cpa
        {% else %}
        , NULL::INTEGER as comparison_cpa
        {% endif %}
        , k.ltv
        {% if user_defined_compare_to_range._is_filtered %}
        , LAG(k.ltv, dd.delta_days) OVER (ORDER BY k."timestamp") AS comparison_ltv
        {% else %}
        , NULL::INTEGER as comparison_ltv
        {% endif %}
        FROM
        kpis k, date_delta dd
      )
      , filtered_rows as (
        select k.*
        from comparison_kpis k
        join date_delta dd
        on k."timestamp" between dd.date_start and dd.date_end
      )
      , moving_averages as (
        select
        "timestamp"
        , AVG(free_trial_created) OVER (
            ORDER BY "timestamp"
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
          ) AS free_trial_created_ma
        {% if user_defined_compare_to_range._is_filtered %}
        , AVG(comparison_free_trial_created) OVER (
            ORDER BY "timestamp"
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
          ) AS comparison_free_trial_created_ma
        {% else %}
        , NULL::INTEGER as comparison_free_trial_created_ma
        {% endif %}
        , AVG(new_trials_14_days_prior) OVER (
            ORDER BY "timestamp"
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
          ) AS new_trials_14_days_prior_ma
        {% if user_defined_compare_to_range._is_filtered %}
        , AVG(comparison_new_trials_14_days_prior) OVER (
            ORDER BY "timestamp"
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
          ) AS comparison_new_trials_14_days_prior_ma
        {% else %}
        , NULL::INTEGER as comparison_new_trials_14_days_prior_ma
        {% endif %}
        , AVG(free_trial_converted) OVER (
            ORDER BY "timestamp"
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
          ) AS free_trial_converted_ma
        {% if user_defined_compare_to_range._is_filtered %}
        , AVG(comparison_free_trial_converted) OVER (
            ORDER BY "timestamp"
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
          ) AS comparison_free_trial_converted_ma
        {% else %}
        , NULL::INTEGER as comparison_free_trial_converted_ma
        {% endif %}
        from comparison_kpis
      )
      , expanded_metrics as (
        select
        "timestamp"
        , SUM(free_trial_created) OVER (ORDER BY "timestamp" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS free_trial_created_running_total
        {% if user_defined_compare_to_range._is_filtered %}
        , SUM(comparison_free_trial_created) OVER (ORDER BY "timestamp" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS comparison_free_trial_created_running_total
        {% else %}
        , NULL::INTEGER as comparison_free_trial_created_running_total
        {% endif %}
        , SUM(free_trial_converted) OVER (ORDER BY "timestamp" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS free_trial_converted_running_total
        {% if user_defined_compare_to_range._is_filtered %}
        , SUM(comparison_free_trial_converted) OVER (ORDER BY "timestamp" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS comparison_free_trial_converted_running_total
        {% else %}
        , NULL::INTEGER as comparison_free_trial_converted_running_total
        {% endif %}
        , SUM(paying_created) OVER (ORDER BY "timestamp" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS paying_created_running_total
        {% if user_defined_compare_to_range._is_filtered %}
        , SUM(comparison_paying_created) OVER (ORDER BY "timestamp" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS comparison_paying_created_running_total
        {% else %}
        , NULL::INTEGER as comparison_paying_created_running_total
        {% endif %}
        , SUM(paying_churn) OVER (ORDER BY "timestamp" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS paying_churn_running_total
        {% if user_defined_compare_to_range._is_filtered %}
        , SUM(comparison_paying_churn) OVER (ORDER BY "timestamp" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS comparison_paying_churn_running_total
        {% else %}
        , NULL::INTEGER as comparison_paying_churn_running_total
        {% endif %}
        from filtered_rows
      )
      select fr.*
      , em.free_trial_created_running_total
      , ma.free_trial_created_ma
      , ma.new_trials_14_days_prior_ma
      , em.comparison_free_trial_created_running_total
      , ma.comparison_free_trial_created_ma
      , ma.comparison_new_trials_14_days_prior_ma
      , em.free_trial_converted_running_total
      , ma.free_trial_converted_ma
      , em.comparison_free_trial_converted_running_total
      , ma.comparison_free_trial_converted_ma
      , em.paying_created_running_total
      , em.comparison_paying_created_running_total
      , em.paying_churn_running_total
      , em.comparison_paying_churn_running_total
      from filtered_rows fr
      join expanded_metrics em
      on fr."timestamp" = em."timestamp"
      join moving_averages ma
      on fr."timestamp" = ma."timestamp"
      order by "timestamp"
      ;;
  }

  # Filter for the primary date range
  filter: user_defined_date_range {
    type: date
    label: "Date Range"
  }

  # Filter for the comparison date range
  filter: user_defined_compare_to_range {
    type: date
    label: "Compare To (Date Range)"
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

  dimension_group: comparison_timestamp {
    type: time
    group_label: "Comparison Dimensions"
    sql: ${TABLE}.comparison_timestamp ;;
  }

  dimension: total_free_trials {
    type: number
    sql: ${TABLE}.total_free_trials ;;
  }

  dimension: comparison_total_free_trials {
    type: number
    group_label: "Comparison Dimensions"
    group_item_label: "Total Free Trials"
    sql: ${TABLE}.comparison_total_free_trials ;;
  }

  dimension: total_paying {
    type: number
    sql: ${TABLE}.total_paying ;;
  }

  dimension: comparison_total_paying {
    type: number
    group_label: "Comparison Dimensions"
    group_item_label: "Total Paying"
    sql: ${TABLE}.comparison_total_paying ;;
  }

  dimension: free_trial_created {
    type: number
    sql: ${TABLE}.free_trial_created ;;
  }

  dimension: free_trial_created_running_total {
    type: number
    sql: ${TABLE}.free_trial_created_running_total ;;
  }

  dimension: free_trial_created_ma {
    type: number
    sql: ${TABLE}.free_trial_created_ma ;;
  }

  dimension: new_trials_14_days_prior_ma {
    type: number
    sql: ${TABLE}.new_trials_14_days_prior_ma ;;
  }

  dimension: comparison_free_trial_created {
    type: number
    group_label: "Comparison Dimensions"
    group_item_label: "Free Trial Created"
    sql: ${TABLE}.comparison_free_trial_created ;;
  }

  dimension: comparison_free_trial_created_running_total {
    type: number
    group_label: "Comparison Dimensions"
    group_item_label: "Free Trial Created Running Total"
    sql: ${TABLE}.comparison_free_trial_created_running_total ;;
  }

  dimension: comparison_free_trial_created_ma {
    type: number
    group_label: "Comparison Dimensions"
    group_item_label: "Free Trial Created moving average"
    sql: ${TABLE}.comparison_free_trial_created_ma ;;
  }

  dimension: comparison_new_trials_14_days_prior_ma {
    type: number
    group_label: "Comparison Dimensions"
    group_item_label: "Free Trial Created 14 days prior moving average"
    sql: ${TABLE}.comparison_new_trials_14_days_prior_ma ;;
  }

  dimension: free_trial_converted {
    type: number
    sql: ${TABLE}.free_trial_converted ;;
  }

  dimension: free_trial_converted_running_total {
    type: number
    sql: ${TABLE}.free_trial_converted_running_total ;;
  }

  dimension: free_trial_converted_ma {
    type: number
    sql: ${TABLE}.free_trial_converted_ma ;;
  }

  dimension: comparison_free_trial_converted {
    type: number
    group_label: "Comparison Dimensions"
    group_item_label: "Free Trial Converted"
    sql: ${TABLE}.comparison_free_trial_converted ;;
  }

  dimension: comparison_free_trial_converted_running_total {
    type: number
    group_label: "Comparison Dimensions"
    group_item_label: "Free Trial Converted (running total)"
    sql: ${TABLE}.comparison_free_trial_converted_running_total ;;
  }

  dimension: comparison_free_trial_converted_ma {
    type: number
    sql: ${TABLE}.comparison_free_trial_converted_ma ;;
  }

  dimension: free_trial_churn {
    type: number
    sql: ${TABLE}.free_trial_churn ;;
  }

  dimension: comparison_free_trial_churn {
    type: number
    group_label: "Comparison Dimensions"
    group_item_label: "Free Trial Churn"
    sql: ${TABLE}.comparison_free_trial_churn ;;
  }

  dimension: paying_created {
    type: number
    sql: ${TABLE}.paying_created ;;
  }

  dimension: paying_created_running_total {
    type: number
    sql: ${TABLE}.paying_created_running_total ;;
  }

  dimension: comparison_paying_created {
    type: number
    group_label: "Comparison Dimensions"
    group_item_label: "Paying Created"
    sql: ${TABLE}.comparison_paying_created ;;
  }

  dimension: comparison_paying_created_running_total {
    type: number
    group_label: "Comparison Dimensions"
    group_item_label: "Paying Created (running total)"
    sql: ${TABLE}.comparison_paying_created_running_total ;;
  }

  dimension: paying_churn {
    type: number
    sql: ${TABLE}.paying_churn ;;
  }

  dimension: paying_churn_running_total {
    type: number
    sql: ${TABLE}.paying_churn_running_total ;;
  }

  dimension: comparison_paying_churn {
    type: number
    group_label: "Comparison Dimensions"
    group_item_label: "Paying Churn"
    sql: ${TABLE}.comparison_paying_churn ;;
  }

  dimension: comparison_paying_churn_running_total {
    type: number
    group_label: "Comparison Dimensions"
    group_item_label: "Paying Churn (running total)"
    sql: ${TABLE}.comparison_paying_churn_running_total ;;
  }

  dimension: paused_created {
    type: number
    sql: ${TABLE}.paused_created ;;
  }

  dimension: comparison_paused_created {
    type: number
    group_label: "Comparison Dimensions"
    group_item_label: "Paused Created"
    sql: ${TABLE}.comparison_paused_created ;;
  }

  dimension: new_trials_14_days_prior {
    type: number
    sql: ${TABLE}.new_trials_14_days_prior ;;
  }

  dimension: comparison_new_trials_14_days_prior {
    type: number
    group_label: "Comparison Dimensions"
    group_item_label: "New Trials 14 Days Prior"
    sql: ${TABLE}.comparison_new_trials_14_days_prior ;;
  }

  dimension: churn_30_days {
    type: number
    sql: ${TABLE}.churn_30_days ;;
  }

  dimension: comparison_churn_30_days {
    type: number
    group_label: "Comparison Dimensions"
    group_item_label: "Churn (30 Days)"
    sql: ${TABLE}.comparison_churn_30_days ;;
  }

  dimension: paying_30_days_prior {
    type: number
    sql: ${TABLE}.paying_30_days_prior ;;
  }

  dimension: comparison_paying_30_days_prior {
    type: number
    group_label: "Comparison Dimensions"
    group_item_label: "Paying 30 days prior"
    sql: ${TABLE}.comparison_paying_30_days_prior ;;
  }

  dimension: day_of_year {
    type: number
    sql: ${TABLE}.day_of_year ;;
  }

  dimension: comparison_day_of_year {
    type: number
    group_label: "Comparison Dimensions"
    group_item_label: "Day of Year"
    sql: ${TABLE}.comparison_day_of_year ;;
  }

  dimension: week {
    type: number
    sql: ${TABLE}.week ;;
  }

  dimension: comparison_week {
    type: number
    group_label: "Comparison Dimensions"
    group_item_label: "Week"
    sql: ${TABLE}.comparison_week ;;
  }

  dimension: month {
    type: number
    sql: ${TABLE}.month ;;
  }

  dimension: comparison_month {
    type: number
    group_label: "Comparison Dimensions"
    group_item_label: "Month"
    sql: ${TABLE}.comparison_month ;;
  }

  dimension: year {
    type: number
    sql: ${TABLE}.year ;;
  }

  dimension: comparison_year {
    type: number
    group_label: "Comparison Dimensions"
    group_item_label: "Year"
    sql: ${TABLE}.comparison_year ;;
  }

  dimension: quarter {
    type: number
    sql: ${TABLE}.quarter ;;
  }

  dimension: comparison_quarter {
    type: number
    group_label: "Comparison Dimensions"
    group_item_label: "Quarter"
    sql: ${TABLE}.comparison_quarter ;;
  }

  dimension: spend {
    type: number
    sql: ${TABLE}.spend ;;
    value_format: "$#.00;($#.00)"
  }

  dimension: comparison_spend {
    type: number
    group_label: "Comparison Dimensions"
    group_item_label: "Spend"
    sql: ${TABLE}.comparison_spend ;;
    value_format: "$#.00;($#.00)"
  }

  dimension: cpa {
    type: number
    sql: ${TABLE}.cpa ;;
    value_format: "$#.00;($#.00)"
  }

  dimension: comparison_cpa {
    type: number
    group_label: "Comparison Dimensions"
    group_item_label: "CPA"
    sql: ${TABLE}.comparison_cpa ;;
    value_format: "$#.00;($#.00)"
  }

  dimension: ltv {
    type: number
    sql: ${TABLE}.ltv ;;
    value_format: "$#.00;($#.00)"
  }

  dimension: comparison_ltv {
    type: number
    group_label: "Comparison Dimensions"
    group_item_label: "LTV"
    sql: ${TABLE}.comparison_ltv ;;
    value_format: "$#.00;($#.00)"
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

  measure: new_trials_running_total {
    type: sum
    label: "New Trials (Running Total)"
    sql: ${free_trial_created_running_total} ;;
  }

  measure: new_trials_moving_avg {
    type: average
    label: "New Trials (Moving Average)"
    sql: ${free_trial_created_ma} ;;
  }

  measure: new_trials_14_days_prior_moving_avg {
    type: average
    label: "New Trials 14 days prior (Moving Average)"
    sql: ${new_trials_14_days_prior_ma} ;;
  }

  measure: new_trials_average {
    type: number
    label: "New Trials (7 day MA TEST)"
    sql: AVG(${new_trials}) OVER (ORDER BY ${timestamp_date} ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) ;;
  }

  measure: comparison_new_trials {
    type: sum
    group_label: "Comparison Measures"
    group_item_label: "New Trials"
    description: "Total number of new trials during comparison period."
    sql:  ${comparison_free_trial_created} ;;
  }

  measure: comparison_total_new_trials_14_days_prior {
    type: sum
    group_label: "Comparison Measures"
    group_item_label: "New Trials (14 days prior)"
    sql: ${comparison_new_trials_14_days_prior};;
  }

  measure: comparison_new_trials_running_total{
    type: sum
    group_label: "Comparison Measures"
    group_item_label: "New Trials (running total)"
    sql: ${comparison_free_trial_created_running_total} ;;
  }

  measure: comparison_new_trials_moving_avg {
    type: average
    group_label: "Comparison Measures"
    group_item_label:"New Trials (Moving Average)"
    sql: ${comparison_free_trial_created_ma} ;;
  }

  measure: comparison_new_trials_14_days_prior_moving_avg {
    type: average
    group_label: "Comparison Measures"
    group_item_label:"New Trials 14 days prior (Moving Average)"
    sql: ${comparison_new_trials_14_days_prior_ma} ;;
  }

  measure: trial_to_paid {
    type: sum
    label: "Trial to Paid"
    description: "Total number of trials converted to paid subscribers during period."
    sql:  ${free_trial_converted} ;;
  }

  measure: trial_to_paid_running_total {
    type: sum
    label: "Trial to Paid (running total)"
    sql: ${free_trial_converted_running_total} ;;
  }

  measure: trial_to_paid_moving_avg {
    type: average
    label: "Trial to Paid (moving average)"
    sql: ${free_trial_converted_ma} ;;
  }

  measure: conversion_rate {
    type: number
    label: "Free Trial Conversion Rate"
    value_format: ".0#\%"
    sql: 100.0*${trial_to_paid}/NULLIF(${total_new_trials_14_days_prior},0) ;;
  }

  measure: conversion_rate_moving_avg {
    type: number
    label: "Free Trial Conversion Rate (moving average)"
    value_format: ".0#\%"
    sql: 100.0*${trial_to_paid_moving_avg}/NULLIF(${new_trials_14_days_prior_moving_avg},0) ;;
  }

  measure: comparison_trial_to_paid {
    type: sum
    group_label: "Comparison Measures"
    group_item_label: "Trial to Paid"
    description: "Total number of trials converted to paid subscribers during period."
    sql:  ${comparison_free_trial_converted} ;;
  }

  measure: comparison_trial_to_paid_running_total {
    type: sum
    group_label: "Comparison Measures"
    group_item_label: "Trial to Paid (running total)"
    sql: ${comparison_free_trial_converted_running_total} ;;
  }

  measure: comparison_trial_to_paid_moving_avg {
    type: average
    group_label: "Comparison Measures"
    group_item_label:"Trial to Paid (moving average)"
    sql: ${comparison_free_trial_converted_ma} ;;
  }

  measure: comparison_conversion_rate {
    type: number
    group_label: "Comparison Measures"
    group_item_label: "Free Trial Conversion Rate"
    value_format: ".0#\%"
    sql: 100.0*${comparison_trial_to_paid}/NULLIF(${comparison_total_new_trials_14_days_prior},0) ;;
  }

  measure: comparison_conversion_rate_moving_avg {
    type: number
    group_label: "Comparison Measures"
    group_item_label: "Free Trial Conversion Rate (moving average)"
    value_format: ".0#\%"
    sql: 100.0*${comparison_trial_to_paid_moving_avg}/NULLIF(${comparison_new_trials_14_days_prior_moving_avg},0) ;;
  }

  measure: new_paid {
    type: sum
    description: "Total number of new paids during period."
    sql:  ${paying_created} ;;
  }

  measure: new_paid_running_total {
    type: sum
    label: "New Paid (running total)"
    sql: ${paying_created_running_total} ;;
  }

  measure: comparison_new_paid {
    type: sum
    group_label: "Comparison Measures"
    group_item_label: "New Paid"
    description: "Total number of new paids during comparison period."
    sql:  ${comparison_paying_created} ;;
  }

  measure: comparison_new_paid_running_total {
    type: sum
    group_label: "Comparison Measures"
    group_item_label: "New Paid (running total)"
    sql:  ${comparison_paying_created_running_total} ;;
  }

  measure: paid_churn {
    type: sum
    sql: ${paying_churn} ;;
  }

  measure: paid_churn_running_total {
    type: sum
    label: "Paid Churn (running total)"
    sql: ${paying_churn_running_total} ;;
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

  measure: comparison_paid_churn {
    type: sum
    group_label: "Comparison Measures"
    group_item_label: "Paid Churn"
    description: "Total number of churned subscribers during comparison period."
    sql:  ${comparison_paying_churn} ;;
  }

  measure: comparison_paid_churn_running_total {
    type: sum
    group_label: "Comparison Measures"
    group_item_label: "Paid Churn (running total)"
    sql:  ${comparison_paying_churn_running_total} ;;
  }

  measure: comparison_churn_30_days_ {
    type: sum
    group_label: "Comparison Measures"
    group_item_label: "Churn 30 Days"
    sql: ${comparison_churn_30_days} ;;
  }

  measure: comparison_paying_30_days_prior_ {
    type: sum
    group_label: "Comparison Measures"
    group_item_label: "Paying_30_days_prior"
    sql: ${comparison_paying_30_days_prior} ;;
  }

  measure: comparison_churn_30_day_percent {
    type: sum
    group_label: "Comparison Measures"
    group_item_label: "Churn Rate"
    sql: ${comparison_churn_30_days}/${comparison_paying_30_days_prior};;
    value_format_name: percent_1
  }

  measure: net_new {
    type: number
    description: "Net new subscribers after trial conversions and paying churn during period"
    sql: ${trial_to_paid}+${new_paid}-${paid_churn} ;;
  }

  measure: comparison_net_new {
    type: number
    group_label: "Comparison Measures"
    group_item_label: "Net new"
    description: "Net new subscribers after trial conversions and paying churn during comparison period"
    sql: ${comparison_trial_to_paid}+${comparison_new_paid}-${comparison_paid_churn} ;;
  }

  measure: net_new_running_total {
    type: sum
    label: "Net new (running total)"
    sql: ${free_trial_converted_running_total}+${paying_created_running_total}-${paying_churn_running_total} ;;
  }

  measure: comparison_net_new_running_total {
    type: sum
    group_label: "Comparison Measures"
    group_item_label: "Net new (running total)"
    sql: ${comparison_free_trial_converted_running_total}+${comparison_paying_created_running_total}-${comparison_paying_churn_running_total} ;;
  }
}
