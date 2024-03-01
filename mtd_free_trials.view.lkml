view: mtd_free_trials {
  derived_table: {
    sql: with customers_analytics as (
    with p0 as (
      select
      analytics_timestamp as timestamp
      , existing_free_trials
      , existing_paying
      , free_trial_churn
      , free_trial_converted
      , free_trial_created
      , paused_created
      , paying_churn
      , paying_created
      , total_free_trials
      , total_paying
      , row_number() over (partition by analytics_timestamp order by sent_at desc) as n
      from php.get_analytics
      where date(sent_at)=current_date
    )
    select
    *
    from p0
    where n=1
  ),

a as
(select a.timestamp,
       free_trial_created,
       total_paying,
       total_free_trials,
       SUM(free_trial_created) OVER (PARTITION by cast(datepart(month,date(timestamp)) as varchar) order by timestamp asc rows between unbounded preceding and current row) AS Running_Free_Trials,
       SUM(free_trial_converted) OVER (PARTITION by cast(datepart(month,date(timestamp)) as varchar) order by timestamp asc rows between unbounded preceding and current row) AS Running_Paid_Conversions,
       SUM(paying_created) OVER (PARTITION by cast(datepart(month,date(timestamp)) as varchar) order by timestamp asc rows between unbounded preceding and current row) AS Running_reacquisitions,
       SUM(paying_churn) OVER (PARTITION by cast(datepart(month,date(timestamp)) as varchar) order by timestamp asc rows between unbounded preceding and current row) AS Running_paid_churn

from customers_analytics as a
--where extract(year from timestamp)=2023
group by 1,
         free_trial_created,
         free_trial_converted,
         paying_churn,
         total_paying,
         total_free_trials,
         paying_created
order by timestamp desc),

b as
(select a1.timestamp,
       case when cast(date_part('month',date(a1.timestamp)) as integer)=1 then (28250*(cast(date_part('day',date(a1.timestamp)) as integer))/30.44)
            when cast(date_part('month',date(a1.timestamp)) as integer)=2 then (28250*(cast(date_part('day',date(a1.timestamp)) as integer))/30.44)
            when cast(date_part('month',date(a1.timestamp)) as integer)=3 then (28250*(cast(date_part('day',date(a1.timestamp)) as integer))/30.44)
            when cast(date_part('month',date(a1.timestamp)) as integer)=4 then (28250*(cast(date_part('day',date(a1.timestamp)) as integer))/30.44)
            when cast(date_part('month',date(a1.timestamp)) as integer)=5 then (28250*(cast(date_part('day',date(a1.timestamp)) as integer))/30.44)
            when cast(date_part('month',date(a1.timestamp)) as integer)=6 then (28250*(cast(date_part('day',date(a1.timestamp)) as integer))/30.44)
            when cast(date_part('month',date(a1.timestamp)) as integer)=7 then (28250*(cast(date_part('day',date(a1.timestamp)) as integer))/30.44)
            when cast(date_part('month',date(a1.timestamp)) as integer)=8 then (28250*(cast(date_part('day',date(a1.timestamp)) as integer))/30.44)
            when cast(date_part('month',date(a1.timestamp)) as integer)=9 then (28250*(cast(date_part('day',date(a1.timestamp)) as integer))/30.44)
            when cast(date_part('month',date(a1.timestamp)) as integer)=10 then (28250*(cast(date_part('day',date(a1.timestamp)) as integer))/30.44)
            when cast(date_part('month',date(a1.timestamp)) as integer)=11 then (28250*(cast(date_part('day',date(a1.timestamp)) as integer))/30.44)
            when cast(date_part('month',date(a1.timestamp)) as integer)=12 then (28250*(cast(date_part('day',date(a1.timestamp)) as integer))/30.44)
            end as mtd_running_trials_target
from customers_analytics as a1
order by 1 desc)

select a.*,
       mtd_running_trials_target
from a inner join b on a.timestamp=b.timestamp
order by timestamp desc
 ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
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

  dimension: free_trial_created {
    type: number
    sql: ${TABLE}.free_trial_created ;;
  }

  measure: free_trial_created_ {
    type: sum
    sql: ${free_trial_created} ;;}



  dimension: running_free_trials {
    type: number
    sql: ${TABLE}.running_free_trials ;;
  }

  measure: running_free_trials_ {
    type: sum
    sql: ${running_free_trials};;
  }

  dimension: running_paid_conversions {
    type: number
    sql: ${TABLE}.running_paid_conversions ;;
  }

  measure: running_paid_conversions_ {
    type: sum
    sql: ${running_paid_conversions};;
  }

  dimension: running_paid_churn {
    type: number
    sql: ${TABLE}.running_paid_churn ;;
  }

  measure: running_paid_churn_ {
    type: sum
    sql: ${running_paid_churn};;
  }

  dimension: running_reacquisitions {
    type: number
    sql: ${TABLE}.running_reacquisitions ;;
  }

  measure: running_reacquisitions_ {
    type: sum
    sql: ${running_reacquisitions};;
  }

  dimension: total_paid {
    type: number
    sql: ${TABLE}.total_paying ;;
  }

  dimension: mtd_running_target_{
    type: number
    sql: ${TABLE}.mtd_running_trials_target  ;;
  }

  measure: mtd_running_target {
    type: sum
    sql: ${mtd_running_target_};;
    value_format: "#,##0"
  }

  set: detail {
    fields: [timestamp_time, running_free_trials, mtd_running_target]
  }
}
