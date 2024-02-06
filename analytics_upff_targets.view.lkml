view: analytics_upff_targets {
  derived_table: {
    sql: WITH monthly_targets AS (
    SELECT '2024-01-01'::DATE AS "month", 30000 AS "new_free_trials", 0.62 AS "paid_conversion_rate", 18600 AS "avg_monthly_paid_ads", 9018 AS "reacquired_subs", 144024 AS "avg_monthly_reacquisition_subs", 0.05 AS "mix_annual_plan_subs", 0.10 AS "churn_rate_monthly_plan", 25 AS "cost_per_acquisition_free_trials", 750000 AS "media_agency_expensed", 40.32 AS "cost_per_acquisition_paid_conversion"
    UNION ALL SELECT '2024-02-01'::DATE, 25000, 0.62, 15500, 7515, 145087, 0.05, 0.10, 25, 625000, 40.32
    UNION ALL SELECT '2024-03-01'::DATE, 30000, 0.62, 18600, 9018, 147990, 0.05, 0.10, 25, 750000, 40.32
    UNION ALL SELECT '2024-04-01'::DATE, 30000, 0.62, 18600, 9018, 150683, 0.05, 0.10, 25, 750000, 40.32
    UNION ALL SELECT '2024-05-01'::DATE, 30000, 0.62, 18600, 9018, 153190, 0.05, 0.10, 25, 750000, 40.32
    UNION ALL SELECT '2024-06-01'::DATE, 30000, 0.62, 18600, 9018, 155528, 0.05, 0.10, 25, 750000, 40.32
    UNION ALL SELECT '2024-07-01'::DATE, 30000, 0.62, 18600, 9018, 157716, 0.05, 0.10, 25, 750000, 40.32
    UNION ALL SELECT '2024-08-01'::DATE, 30000, 0.62, 18600, 9018, 159769, 0.05, 0.10, 25, 750000, 40.32
    UNION ALL SELECT '2024-09-01'::DATE, 30000, 0.62, 18600, 9018, 161703, 0.05, 0.10, 25, 750000, 40.32
    UNION ALL SELECT '2024-10-01'::DATE, 30000, 0.62, 18600, 9018, 163529, 0.05, 0.10, 25, 750000, 40.32
    UNION ALL SELECT '2024-11-01'::DATE, 30000, 0.62, 18600, 9018, 165259, 0.05, 0.10, 25, 750000, 40.32
    UNION ALL SELECT '2024-12-01'::DATE, 25000, 0.62, 15500, 7515, 165021, 0.05, 0.10, 25, 625000, 40.32
    )
    SELECT
    month,
    new_free_trials,
    paid_conversion_rate,
    avg_monthly_paid_ads,
    reacquired_subs,
    avg_monthly_reacquisition_subs,
    mix_annual_plan_subs,
    churn_rate_monthly_plan,
    cost_per_acquisition_free_trials,
    media_agency_expensed,
    cost_per_acquisition_paid_conversion
    FROM
    monthly_targets;;
  }

  dimension: month {
    type: date_month
    primary_key: yes
    sql: ${TABLE}.month ;;
  }

  dimension: new_free_trials {
    type: number
    sql: ${TABLE}.new_free_trials ;;
  }

  dimension: paid_conversion_rate {
    type: number
    value_format_name: percent_1
    sql: ${TABLE}.paid_conversion_rate ;;
  }

  dimension: avg_monthly_paid_ads {
    type: number
    sql: ${TABLE}.avg_monthly_paid_ads ;;
  }

  dimension: reacquired_subs {
    type: number
    sql: ${TABLE}.reacquired_subs ;;
  }

  dimension: avg_monthly_reacquisition_subs {
    type: number
    sql: ${TABLE}.avg_monthly_reacquisition_subs ;;
  }

  dimension: mix_annual_plan_subs {
    type: number
    value_format_name: percent_1
    sql: ${TABLE}.mix_annual_plan_subs ;;
  }

  dimension: churn_rate_monthly_plan {
    type: number
    value_format_name: percent_1
    sql: ${TABLE}.churn_rate_monthly_plan ;;
  }

  dimension: cost_per_acquisition_free_trials {
    type: number
    sql: ${TABLE}.cost_per_acquisition_free_trials ;;
  }

  dimension: media_agency_expensed {
    type: number
    sql: ${TABLE}.media_agency_expensed ;;
  }

  dimension: cost_per_acquisition_paid_conversion {
    type: number
    sql: ${TABLE}.cost_per_acquisition_paid_conversion ;;
  }

  measure: avg_new_free_trials {
    type: average
    sql: ${new_free_trials} ;;
  }

  measure: avg_paid_conversion_rate {
    type: average
    sql: ${paid_conversion_rate} ;;
  }

  measure: avg_avg_monthly_paid_ads {
    type: average
    sql: ${avg_monthly_paid_ads} ;;
  }

  measure: avg_reacquired_subs {
    type: average
    sql: ${reacquired_subs} ;;
  }

  measure: average_monthly_reacquisition_subs {
    type: average
    sql: ${avg_monthly_reacquisition_subs} ;;
  }

  measure: avg_mix_annual_plan_subs_percent {
    type: average
    sql: ${mix_annual_plan_subs} ;;
  }

  measure: avg_churn_rate_monthly_plan_percent {
    type: average
    sql: ${churn_rate_monthly_plan} ;;
  }

  measure: avg_cost_per_acquisition_free_trials {
    type: average
    sql: ${cost_per_acquisition_free_trials} ;;
  }

  measure: avg_media_agency_expensed {
    type: average
    sql: ${media_agency_expensed} ;;
  }

  measure: avg_cost_per_acquisition_paid_conversion {
    type: average
    sql: ${cost_per_acquisition_paid_conversion} ;;
  }
}
