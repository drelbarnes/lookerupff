view: mtd_free_trials {
  derived_table: {
    sql: with customers_analytics as (select analytics_timestamp as timestamp,
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
where date(sent_at)=current_date)

select a.timestamp, free_trial_created, SUM(free_trial_created) OVER (PARTITION by cast(datepart(month,date(timestamp)) as varchar) order by timestamp asc rows between unbounded preceding and current row) AS Running_Free_Trials
from customers_analytics as a
where extract(year from timestamp)=2019
group by 1,free_trial_Created
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

  set: detail {
    fields: [timestamp_time, running_free_trials]
  }
}
