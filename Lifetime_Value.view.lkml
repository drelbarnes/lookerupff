view: lifetime_value {
  derived_table: {
    sql:select cast(churn_30_days as decimal) as churn_30_days, cast(total_paying as decimal) as total_paying_31_days_prior
from
(select sum(paying_churn) as churn_30_days, 1 as matching
from customers.analytics
      where   (((analytics.timestamp ) >= ((DATEADD(day,-29, DATE_TRUNC('day',GETDATE()) ))) AND (analytics.timestamp ) < ((DATEADD(day,30, DATEADD(day,-29, DATE_TRUNC('day',GETDATE()) ) )))))) as a
inner join
(select analytics.timestamp, total_paying, 1 as matching from customers.analytics where timestamp= ((DATEADD(day,-30, DATE_TRUNC('day',GETDATE()) )))) as b
on a.matching=b.matching

 ;;
  }

  dimension: churn_30_days {
    type: number
    sql: ${TABLE}.churn_30_days ;;
  }

  dimension: total_paying_31_days_prior {
    type: number
    sql: ${TABLE}.total_paying_31_days_prior ;;
  }

  measure: churn_percent {
    type: number
    value_format_name: decimal_4
    sql: ${churn_30_days}/${total_paying_31_days_prior} ;;
  }

  measure: lifetime_value {
    type: number
    value_format_name: usd
    sql: 5.99/(${churn_30_days}/${total_paying_31_days_prior});;
  }
}
