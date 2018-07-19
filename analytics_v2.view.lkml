view: analytics_v2 {
  derived_table: {
    sql: with a as (select a.timestamp, ROW_NUMBER() OVER(ORDER BY a.timestamp desc) AS Row
           from customers.analytics as a),

     b as (select a.timestamp,total_paying,ROW_NUMBER() OVER(ORDER BY a.timestamp desc) AS Row
           from customers.analytics as a where a.timestamp < (DATEADD(day,-31, DATE_TRUNC('day',GETDATE()) ))),

     c as (select a.timestamp,total_paying as paying_30_days_prior from a inner join b on a.row=b.row),

     d as ((select a1.timestamp, a1.paying_churn+sum(coalesce(a2.paying_churn,0)) as churn_30_days
from customers.analytics as a1
left join customers.analytics as a2 on datediff(day,a2.timestamp,a1.timestamp)<=30 and datediff(day,a2.timestamp,a1.timestamp)>0
group by a1.timestamp,a1.paying_churn)),

     e as (select c.timestamp, cast(paying_30_days_prior as decimal) as paying_30_days_prior,
                               cast(churn_30_days as decimal) as churn_30_days,
                               cast(paying_30_days_prior as decimal)/cast(churn_30_days as decimal) as churn_30_day_percent
           from c inner join d on c.timestamp=d.timestamp),

     f as (select *, sum((49000-(total_paying))/(365-day_of_year)) OVER (PARTITION by cast(datepart(month,date(timestamp)) as varchar) order by timestamp asc
                 rows between unbounded preceding and current row) as Running_Free_Trial_Target
         from (select *, SUM(free_trial_created) OVER (PARTITION by cast(datepart(month,date(timestamp)) as varchar) order by timestamp asc rows between unbounded preceding and current row) AS Running_Free_Trials
         from (select distinct * from (select a.*,
                795+((49000-795)*(cast(datepart(dayofyear,date(a.timestamp)) as integer)-1)/365) as target,
                795+((49000-795)*(cast(datepart(dayofyear,date(a.timestamp)) as integer)+14)/365) as target_14_days_future,
                cast(datepart(dayofyear,date(a.timestamp)) as integer)-1 as day_of_year,
                cast(datepart(dayofyear,date(a.timestamp)) as integer)+14 as day_of_year_14_days,
                49000 as annual_target,
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
      left join customers.churn_reasons_aggregated as b on a.timestamp=b.timestamp)) as a))

      select f.*,paying_30_days_prior,churn_30_days,churn_30_day_percent from e inner join f on e.timestamp=f.timestamp ;;}

dimension: paying_30_days_prior {
  type: number
  sql: ${TABLE}.paying_30_days_prior ;;
}

dimension: churn_30_days {
  type: number
  sql: churn_30_days ;;
}

dimension: running_free_trials {
  type: number
  sql: ${TABLE}.Running_Free_Trials ;;
}

measure: running_free_trials_ {
  type: sum
  sql: ${running_free_trials} ;;
}

  dimension: running_free_trial_target {
    type: number
    sql: ${TABLE}.running_free_trial_target*2 ;;
  }

  measure: running_free_trial_target_{
    type: sum
    sql: ${running_free_trial_target} ;;
  }

dimension: target {
  type: number
  sql: ${TABLE}.target ;;
}

measure: targets {
  type: sum
  sql: ${target} ;;
}

  dimension: target_14_days_future {
    type: number
    sql: ${TABLE}.target_14_days_future ;;
  }

  measure: target_14_days_future_ {
    type: sum
    sql: ${target_14_days_future} ;;
  }

  dimension: annual_target {
    type: number
    sql: ${TABLE}.annual_target ;;
  }

  measure: annual_targets {
    type: sum
    sql: ${annual_target} ;;
  }

  dimension: day_of_year {
    type: number
    sql: ${TABLE}.day_of_year ;;
  }

  dimension: day_of_year_14 {
    type: number
    sql: ${TABLE}.day_of_year_14_days ;;
  }

  dimension: avg_target_subs_per_day {
    type:  number
    sql: (${annual_target}-(${TABLE}.total_paying))/(365-${TABLE}.day_of_year);;
  }

  measure: avg_targets_subs_per_day {
    type:  sum
    sql: ${avg_target_subs_per_day};;
  }

  measure: avg_targets_trials_per_day {
    type:  sum
    sql: ${avg_target_subs_per_day}*2;;
  }

  dimension: avg_target_subs_per_day_14_days {
    type:  number
    sql: (365-${TABLE}.day_of_year_14_days);;
  }

  measure: avg_targets_subs_per_day_14_days_ {
    type:  sum
    sql: ${avg_target_subs_per_day_14_days};;
  }

  measure: running_target {
    type: running_total
    sql: ${avg_target_subs_per_day_14_days} ;;
  }

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
    drill_fields: [high_price,timestamp_date]
  }

  measure: other_total {
    type: sum
    sql: ${TABLE}.other ;;
    drill_fields: [other,timestamp_date]
  }

  measure: save_money_total {
    type: sum
    sql: ${TABLE}.save_money ;;
    drill_fields: [save_money,timestamp_date]
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
    drill_fields: [vacation,timestamp_date]
  }

  measure: wait_content_total {
    type: sum
    sql: ${TABLE}.wait_content ;;
    drill_fields: [wait_content,timestamp_date]
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
    drill_fields: [new_trials_14_days_prior,timestamp_date]
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



  dimension: free_trial_converted {
    type: number
    sql: ${TABLE}.free_trial_converted ;;
  }

  measure: trial_to_paid {
    type: sum
    description: "Total number of trials to paid during a time period."
    sql:  ${free_trial_converted} ;;
    drill_fields: [free_trial_converted,timestamp_date]

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
    drill_fields: [paying_created,timestamp_date]
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

  measure: total_count {
    type: sum
    description: "Total number of existing free trials and paid subs during a period of time"
    sql:  ${existing_paying}+${existing_free_trials} ;;
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

  measure: trial_to_paid_count {
    type: number
    description: "Total number of trials to paid during a time period."
    sql:  COUNT(${free_trial_converted}) ;;
    drill_fields: [free_trial_converted,timestamp_date]

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

  measure: total_free_trial_change {
    type: number
    sql: (${free_trials_total}-${free_trials_count});;
  }

  measure: total_paid_sub_change {
    type: number
    sql: (${paying_total}-${paid_subs_count});;
  }

  measure: net_gained {
    type: number
    sql: (${new_trials}+${trial_to_paid}+${new_paid})+(${cancelled_trials}+${Cancelled_Subs}) ;;
  }

  measure: net_paid {
    type: number
    sql: (${trial_to_paid}+${new_paid})+(${Cancelled_Subs}) ;;
  }

  measure: net_trials {
    type: number
    sql: (${new_trials})+(${cancelled_trials}-${trial_to_paid}) ;;
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

  measure: paid_a {
    type: sum
    sql:  ${total_paying};;
    filters: {
      field: group_a
      value: "yes"
    }
  }

  measure: trials_a {
    type: sum
    sql:  ${total_free_trials};;
    filters: {
      field: group_a
      value: "yes"
    }
  }

  measure: conversions_a {
    type: sum
    sql: ${free_trial_converted};;
    filters: {
      field: group_a
      value: "yes"
    }
  }

  measure: reacquisitions_a {
    type: sum
    sql: ${paying_created};;
    filters: {
      field: group_a
      value: "yes"
    }
  }

  measure: paid_churn_a {
    type: sum
    sql: ${paying_churn} ;;
    filters: {
      field: group_a
      value: "yes"
    }
  }


  measure: churn_30_day_percent_b {
    type: sum
    sql: ${churn_30_days}/${paying_30_days_prior};;
    value_format_name: percent_0
    filters: {
      field: group_b
      value: "yes"
    }
  }

  measure: trial_churn_a {
    type: sum
    sql: ${free_trial_churn} ;;
    filters: {
      field: group_a
      value: "yes"
    }
  }

  measure: trial_starts_a {
    type: sum
    sql: ${free_trial_created} ;;
    filters: {
      field: group_a
      value: "yes"
    }
  }


  measure: churn_percent_b {
    type: sum
    sql: ${TABLE}.churn_30_day_percent ;;
    filters: {
      field: group_b
      value: "yes"
    }
  }
    measure: avg_paid_b {
      type: average
      sql:  ${total_paying};;
      filters: {
        field: group_b
        value: "yes"
      }
    }

    measure: paid_change {
      type: number
      sql: (${paid_a}-${avg_paid_b}) ;;
    }

  measure: avg_trials_b {
    type: average
    sql:  ${total_free_trials};;
    filters: {
      field: group_b
      value: "yes"
    }
  }

  measure: trials_change {
    type: number
    sql: (${trials_a}-${avg_trials_b}) ;;
  }

  measure: avg_conversions_b {
    type: average
    sql:  ${free_trial_converted};;
    filters: {
      field: group_b
      value: "yes"
    }
  }

  measure: conversion_change {
    type: number
    sql: (${conversions_a}-${avg_conversions_b}) ;;
  }

  measure: avg_reacquisitions_b {
    type: average
    sql:  ${paying_created};;
    filters: {
      field: group_b
      value: "yes"
    }
  }

  measure: reacquisition_change {
    type: number
    sql: (${reacquisitions_a}-${avg_reacquisitions_b}) ;;
  }

  measure: avg_paid_churn_b {
    type: average
    sql:  ${paying_churn};;
    filters: {
      field: group_b
      value: "yes"
    }
  }

  measure: paid_churn_change {
    type: number
    sql: (${paid_churn_a}-${avg_paid_churn_b}) ;;
  }

measure: avg_trial_churn_b {
  type: average
  sql:  ${free_trial_churn};;
  filters: {
    field: group_b
    value: "yes"
  }
}

  measure: trial_churn_change {
    type: number
    sql: (${trial_churn_a}-${avg_trial_churn_b}) ;;
  }

  measure: avg_trial_starts_b {
    type: average
    sql:  ${free_trial_created};;
    filters: {
      field: group_b
      value: "yes"
    }
  }

  measure: trials_created_change {
    type: number
    sql: (${trial_starts_a}-${avg_trial_starts_b}) ;;
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

  parameter: date_granularity {
    type: string
    allowed_value: { value: "Day" }
    allowed_value: { value: "Week"}
    allowed_value: { value: "Month" }
    allowed_value: { value: "Quarter" }
    allowed_value: { value: "Year" }
  }

  dimension: date {
    label_from_parameter: date_granularity
    sql:
       CASE
         WHEN {% parameter date_granularity %} = 'Day' THEN
           ${timestamp_date}::VARCHAR
         WHEN {% parameter date_granularity %} = 'Week' THEN
           ${timestamp_week}::VARCHAR
         WHEN {% parameter date_granularity %} = 'Month' THEN
           ${timestamp_month}::VARCHAR
         WHEN {% parameter date_granularity %} = 'Quarter' THEN
           ${timestamp_quarter}::VARCHAR
         WHEN {% parameter date_granularity %} = 'Year' THEN
           ${timestamp_year}::VARCHAR
         ELSE
           NULL
       END ;;
  }

}
