view: customer_churn_percent {
  derived_table: {
    sql:with

customers_analytics as (select analytics_timestamp as timestamp,
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

a as (select date(customer_created_at) as customer_created_at, date(event_created_at) as event_created_at,platform,count(*) as churn_count
from customers.customers
where (customers.status  IN ('cancelled', 'disabled', 'expired', 'refunded'))
group by 1,2,3),

b as (select date(a.timestamp) as timestamp, free_trial_created
from customers_analytics as a)

select customer_created_at,
case
        when (DATEDIFF('day', (DATE(customer_created_at)), (DATE(event_created_at))))<=14 then '0-14 Days'
        when (DATEDIFF('day', (DATE(customer_created_at)), (DATE(event_created_at))))>14 and (DATEDIFF('day', (DATE(customer_created_at)), (DATE(event_created_at))))<=28 then '15-28 Days'
        when (DATEDIFF('day', (DATE(customer_created_at)), (DATE(event_created_at))))>28 and (DATEDIFF('day', (DATE(customer_created_at)), (DATE(event_created_at))))<=35 then '29-35 Days'
        when (DATEDIFF('day', (DATE(customer_created_at)), (DATE(event_created_at))))>35 and (DATEDIFF('day', (DATE(customer_created_at)), (DATE(event_created_at))))<=42 then '36-42 Days'
        when (DATEDIFF('day', (DATE(customer_created_at)), (DATE(event_created_at))))>42 and (DATEDIFF('day', (DATE(customer_created_at)), (DATE(event_created_at))))<=49 then '43-49 Days'
        when (DATEDIFF('day', (DATE(customer_created_at)), (DATE(event_created_at))))>49 and (DATEDIFF('day', (DATE(customer_created_at)), (DATE(event_created_at))))<=56 then '49-56 Days'
        else '56+ Days'
        end AS "days_since_creation",
sum(cast(churn_count as decimal)) as churn_count,
cast(free_trial_created as decimal) as free_trial_created
from a inner join b on customer_created_at=b.timestamp
group by 1,2,4;;
  }

dimension:customer_created_at{
type: date
sql: ${TABLE}.customer_created_at ;;
}

  dimension_group: creation_timestamp {
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
    sql: ${TABLE}.customer_created_at ;;
  }

  dimension: days_since_creation{
    type: string
    sql:  ${TABLE}.days_since_creation;;
  }

  measure: free_trial_created {
    type: sum
    sql:  ${TABLE}.free_trial_created ;;
  }

  measure: churn_count {
    type: sum
    sql: ${TABLE}.churn_count ;;
  }

  measure: churn_percent_v2 {
    type: number
    sql: ${churn_count}/${free_trial_created} ;;
    value_format_name: percent_0
  }

}
