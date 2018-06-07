view: customer_churn_percent {
  derived_table: {
    sql: with

a as (select date(customer_created_at) as customer_created_at, date(event_created_at) as event_created_at,count(*) as churn_count
from customers.customers
where (customers.status  IN ('cancelled', 'disabled', 'expired', 'paused', 'refunded'))
group by 1,2),

b as (select date(a.timestamp) as timestamp, free_trial_created
from customers.analytics as a)

select customer_created_at,event_created_at,cast(churn_count as decimal) as churn_count,
cast(free_trial_created as decimal) as free_trial_created, cast(churn_count as decimal)/cast(free_trial_created as decimal) as churn_percent
from a inner join b on customer_created_at=b.timestamp ;;
  }

  dimension: customer_created_at {
    type: date
    sql: ${TABLE}.customer_created_at;;
  }

  dimension: event_created_at {
    type: date
    sql: ${TABLE}.event_created_at;;
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

  dimension: days_since_created {
    type: number
    sql:  DATEDIFF('day', ${customer_created_at}, ${event_created_at});;
  }

  measure: free_trial_created {
    type: sum
    sql: ${TABLE}.free_trial_created ;;
  }

  measure: churn_count {
    type: sum
    sql: ${TABLE}.churn_count ;;
  }

  measure: churn_percent {
    type: sum
    sql: ${TABLE}.churn_percent ;;
    value_format_name: percent_0
  }

  measure: churn_percent_v2 {
    type: number
    sql: ${churn_count}/${free_trial_created} ;;
    value_format_name: percent_0
  }

  dimension: days_since_creation{
    type: string
    sql:
      case
        when ${days_since_created}<=14 then '0-14 Days'
        when ${days_since_created}>14 and ${days_since_created}<=28 then '15-28 Days'
        when ${days_since_created}>28 and ${days_since_created}<=35 then '29-35 Days'
        when ${days_since_created}>35 and ${days_since_created}<=42 then '36-42 Days'
        when ${days_since_created}>42 and ${days_since_created}<=49 then '43-49 Days'
        when ${days_since_created}>49 and ${days_since_created}<=56 then '49-56 Days'
        else '56+ Days'
        end;;
  }

}
