view: redshift_ribbow_agency_fee {
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
where date(sent_at)=current_date),

a as
(select date(timestamp) as timestamp,
       1.5*(free_trial_converted+paying_created) as agency_fee
from customers_analytics
order by 1 desc)

select TO_CHAR(DATE_TRUNC('month', timestamp), 'YYYY-MM'),
      sum(agency_fee) as agency_fee
from a
group by 1
order by 1 desc
 ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: to_char {
    type: string
    sql: ${TABLE}.to_char ;;
  }

  dimension: agency_fee {
    type: number
    sql: ${TABLE}.agency_fee ;;
  }

  set: detail {
    fields: [to_char, agency_fee]
  }
}
