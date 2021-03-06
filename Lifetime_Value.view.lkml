view: lifetime_value {
  derived_table: {
    sql:select a.timestamp,cast(churn_30_days as decimal) as churn_30_days,cast(prior_31_days_subs as decimal) as prior_31_days_subs, 3.40/(cast(churn_30_days as decimal)/cast(prior_31_days_subs as decimal)) as LTV
from
(select a1.timestamp, a1.paying_churn+sum(coalesce(a2.paying_churn,0)) as churn_30_days
from customers.analytics as a1
left join customers.analytics as a2 on datediff(day,a2.timestamp,a1.timestamp)<=30 and datediff(day,a2.timestamp,a1.timestamp)>0
group by a1.timestamp,a1.paying_churn) as a
inner join
(select a.timestamp,total_paying as prior_31_days_subs
from
(select a.timestamp, ROW_NUMBER() OVER(ORDER BY a.timestamp desc) AS Row from customers.analytics as a) as a
inner join
(select a.timestamp,total_paying, ROW_NUMBER() OVER(ORDER BY a.timestamp desc) AS Row from customers.analytics as a where (a.timestamp  < (DATEADD(day,-32, DATE_TRUNC('day',GETDATE()) )))) as b
on a.row=b.row) as b
on a.timestamp=b.timestamp
;;
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

  dimension: churn_30_days {
    type: number
    sql: ${TABLE}.churn_30_days ;;
  }

  dimension: total_paying_31_days_prior {
    type: number
    sql: ${TABLE}.prior_31_days_subs ;;
  }

  dimension: churn_percent{
    type:  number
    sql: ${churn_30_days}/${total_paying_31_days_prior} ;;
  }

  measure: churn_30_days_ {
    type: sum
    sql: ${TABLE}.churn_30_days ;;
  }

  measure: total_paying_31_days_prior_ {
    type: sum
    sql: ${TABLE}.prior_31_days_subs ;;
  }

  measure: churn_percent_{
    type:  number
    sql: ${churn_30_days_}/${total_paying_31_days_prior_} ;;
    value_format_name: percent_1
  }

  measure: lifetime_value {
    type: sum
    value_format_name: usd
    sql:${TABLE}.LTV;;
  }
}
